# Instalación

## Prerequisitos

- Python 3.10+
- Node.js
- AWS CLI configurado

## Instalación

```bash
# Instalar CDK
npm install -g aws-cdk

# Instalar dependencias Python
pip install -r requirements.txt

# Bootstrap CDK (primera vez)
cdk bootstrap
```

## Despliegue

```bash
cdk deploy --require-approval never
```