from aws_cdk import (
    Duration,
    Stack,
    aws_s3 as s3,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_glue as glue,
    aws_lakeformation as lakeformation,
    aws_athena as athena,
    aws_events as events,
    aws_events_targets as targets,
    aws_apigateway as apigateway,
    aws_dynamodb as dynamodb,
    RemovalPolicy,
)
from constructs import Construct
import json

class DataPipelineStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # S3 Bucket para almacenar los datos
        self.data_bucket = s3.Bucket(
            self, "DataBucket",
            bucket_name=f"data-pipeline-bucket-{self.account}-{self.region}",
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        # S3 Bucket para resultados de Athena
        self.athena_results_bucket = s3.Bucket(
            self, "AthenaResultsBucket",
            bucket_name=f"athena-results-bucket-{self.account}-{self.region}",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        # Rol IAM para Lambda
        self.lambda_role = iam.Role(
            self, "LambdaExecutionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
            ]
        )

        # Permisos para Lambda escribir y leer en S3
        self.data_bucket.grant_read_write(self.lambda_role)

        # DynamoDB table for job tracking
        self.jobs_table = dynamodb.Table(
            self, "JobsTable",
            table_name=f"data-pipeline-jobs-{self.account}",
            partition_key=dynamodb.Attribute(
                name="job_id",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY
        )

        # Grant Lambda permissions to DynamoDB
        self.jobs_table.grant_read_write_data(self.lambda_role)

        # Función Lambda para extracción de datos
        self.data_extractor_lambda = _lambda.Function(
            self, "DataExtractorLambda",
            runtime=_lambda.Runtime.PYTHON_3_10,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("lambda"),
            role=self.lambda_role,
            timeout=Duration.minutes(5),
            environment={
                "BUCKET_NAME": self.data_bucket.bucket_name,
                "API_URL": "https://jsonplaceholder.typicode.com/users"
            }
        )

        # API Lambda function for REST endpoints
        self.api_lambda = _lambda.Function(
            self, "ApiLambda",
            runtime=_lambda.Runtime.PYTHON_3_10,
            handler="api_handler.lambda_handler",
            code=_lambda.Code.from_asset("lambda"),
            role=self.lambda_role,
            timeout=Duration.minutes(5),
            environment={
                "BUCKET_NAME": self.data_bucket.bucket_name,
                "TABLE_NAME": self.jobs_table.table_name
            }
        )

        # API Gateway
        self.api = apigateway.RestApi(
            self, "DataPipelineApi",
            rest_api_name="Data Pipeline API",
            description="REST API for testing the data pipeline",
            default_cors_preflight_options=apigateway.CorsOptions(
                allow_origins=apigateway.Cors.ALL_ORIGINS,
                allow_methods=apigateway.Cors.ALL_METHODS,
                allow_headers=["Content-Type", "X-Amz-Date", "Authorization", "X-Api-Key", "X-Amz-Security-Token"]
            )
        )

        # API Gateway Lambda integration
        api_integration = apigateway.LambdaIntegration(
            self.api_lambda,
            request_templates={"application/json": '{"statusCode": "200"}'}
        )

        # API Routes
        # POST /process
        process_resource = self.api.root.add_resource("process")
        process_resource.add_method("POST", api_integration)

        # GET /status/{job_id}
        status_resource = self.api.root.add_resource("status")
        status_job_resource = status_resource.add_resource("{job_id}")
        status_job_resource.add_method("GET", api_integration)

        # GET /results
        results_resource = self.api.root.add_resource("results")
        results_resource.add_method("GET", api_integration)

        # GET /health
        health_resource = self.api.root.add_resource("health")
        health_resource.add_method("GET", api_integration)

        # Base de datos de Glue
        self.glue_database = glue.CfnDatabase(
            self, "GlueDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="data_pipeline_db",
                description="Database for data pipeline"
            )
        )

        # Rol IAM para Glue Crawler
        self.glue_role = iam.Role(
            self, "GlueServiceRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
            ]
        )

        # Permisos para Glue leer de S3
        self.data_bucket.grant_read(self.glue_role)

        # Glue Crawler
        self.glue_crawler = glue.CfnCrawler(
            self, "GlueCrawler",
            name="data-pipeline-crawler",
            role=self.glue_role.role_arn,
            database_name=self.glue_database.ref,
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path=f"s3://{self.data_bucket.bucket_name}/data/"
                    )
                ]
            ),
            schedule=glue.CfnCrawler.ScheduleProperty(
                schedule_expression="cron(0 2 * * ? *)"
            )
        )

        # Configuración de Athena Workgroup
        self.athena_workgroup = athena.CfnWorkGroup(
            self, "AthenaWorkGroup",
            name="data-pipeline-workgroup",
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f"s3://{self.athena_results_bucket.bucket_name}/"
                )
            )
        )

        # Rol IAM para Athena
        self.athena_role = iam.Role(
            self, "AthenaExecutionRole",
            assumed_by=iam.ServicePrincipal("athena.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonAthenaFullAccess")
            ]
        )

        # Permisos para Athena
        self.data_bucket.grant_read(self.athena_role)
        self.athena_results_bucket.grant_read_write(self.athena_role)

        # EventBridge rule para ejecutar Lambda cada hora
        lambda_schedule = events.Rule(
            self, "LambdaScheduleRule",
            schedule=events.Schedule.rate(Duration.hours(1))
        )
        lambda_schedule.add_target(targets.LambdaFunction(self.data_extractor_lambda))

        # Permisos para EventBridge invocar Lambda
        self.data_extractor_lambda.add_permission(
            "AllowEventBridge",
            principal=iam.ServicePrincipal("events.amazonaws.com"),
            action="lambda:InvokeFunction",
            source_arn=lambda_schedule.rule_arn
        )

        # Lake Formation - Configuración de permisos de datos
        # Nota: Lake Formation requiere configuración manual adicional en la consola
        # para establecer permisos granulares a nivel de tabla y columna
        
        # Permisos adicionales para Glue acceder a Lake Formation
        self.glue_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "lakeformation:GetDataAccess",
                    "lakeformation:GrantPermissions",
                    "lakeformation:BatchGrantPermissions",
                    "lakeformation:RevokePermissions",
                    "lakeformation:BatchRevokePermissions",
                    "lakeformation:ListPermissions"
                ],
                resources=["*"]
            )
        )

        # Permisos adicionales para Athena acceder a Glue Catalog
        self.athena_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "glue:GetDatabase",
                    "glue:GetDatabases",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:GetPartition",
                    "glue:GetPartitions",
                    "glue:BatchCreatePartition",
                    "glue:BatchDeletePartition",
                    "glue:BatchUpdatePartition"
                ],
                resources=[
                    f"arn:aws:glue:{self.region}:{self.account}:catalog",
                    f"arn:aws:glue:{self.region}:{self.account}:database/{self.glue_database.ref}",
                    f"arn:aws:glue:{self.region}:{self.account}:table/{self.glue_database.ref}/*"
                ]
            )
        )

        # Output values para referencia
        from aws_cdk import CfnOutput
        
        CfnOutput(
            self, "DataBucketName",
            value=self.data_bucket.bucket_name,
            description="Nombre del bucket S3 para datos"
        )
        
        CfnOutput(
            self, "GlueDatabaseName",
            value=self.glue_database.ref,
            description="Nombre de la base de datos de Glue"
        )
        
        CfnOutput(
            self, "LambdaFunctionName",
            value=self.data_extractor_lambda.function_name,
            description="Nombre de la función Lambda"
        )
        
        CfnOutput(
            self, "AthenaWorkGroupName",
            value=self.athena_workgroup.name,
            description="Nombre del WorkGroup de Athena"
        )
        
        CfnOutput(
            self, "ApiGatewayUrl",
            value=self.api.url,
            description="URL base del API Gateway para testing"
        )
        
        CfnOutput(
            self, "JobsTableName",
            value=self.jobs_table.table_name,
            description="Nombre de la tabla DynamoDB para jobs"
        )