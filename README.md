# Test Technique

## Présentation

Vous faites partie de l'équipe Backend de Bylaw, félicitations ! Vous rejoignez l'équipe "Notaires". Vous et votre équipe allez prendre en charge le développement d'une API de classification de documents. Cette API permettra aux notaires de classer leurs documents.

### Objectif

Une première version de l'API d'analyse a déjà été développée. Votre tâche est de l'intégrer dans une pipeline d'analyse. 
Cette pipeline doit pouvoir recevoir des documents, en extraire le texte, les analyser et les stocker.

Une fois la pipeline d'analyse terminée en local, vous devrez l'intégrer à des services AWS (S3, DynamoDB, SQS, CloudWatch) pour permettre le traitement de documents en production. 
Voilà une liste de services que vous pouvez mettre en place :

- DynamoDB : Stockage des documents et de leurs métadonnées
- S3 : Stockage des documents
- SQS : File d'attente pour le traitement des documents
- CloudWatch : Logs pour suivre l'activité de l'API

### Critères de réussite

Vous serez jugé sur les critères suivants :
- Qualité du code et de son architecture
- Organisation du stockage des documents et des logs
- Test des résultats et fonctions du code
- Utilisation de DynamoDB pour un accès rapide en cas de grand nombre d'objets
- Optimisation pour pouvoir process un maximum de documents

### Présentation du dossier

- Le programme principal à modifier : `main.py`
- Le fichier pour l'API (pas besoin d'y toucher) : `api.py`
- Le fichier des librairies Python nécessaires : `requirements.txt`
- Le dossier avec les documents PDF de test : `documents/`
- Le dossier dans lequel va se retrouver les documents classifiés : `results/`

### Conditions de rendu

Il faudra compresser le dossier actuel avec vos modifications dans un zip et nous l'envoyer :
`tuan@bylaw.fr` et `s.boulet@bylaw.fr`
Nous vous répondrons pour organiser une review via visio.

### Documentation de l'API

- `POST` /ocr
Demande l'OCR d'un document PDF envoyé dans la requête
Requête :
```
files = [(
    'pdf',
    (filename, binary, 'application/pdf')
)]
```
Réponse :
```
{
    "pages": [
        "String"
    ]
}
```

- `POST` /score
Demande la classification à partir de l'OCR de pages envoyé dans la requête
Requête :
```
json = {
    "pages": [
        "String"
    ]
}
```
Réponse :
```
{
    'segmentDiag': [
        {
            'categorie': "String",
            'pages': [Int, Int]
        }
    ], 'segmentOp': [
        {
            'categorie': "String",
            'pages': [Int, Int]
        }
    ]
}
```

## Mise en place

### Prérequis

- Installer et créer un environnement virtuel :

  ```shell
  pip install virtualenv
  virtualenv venv
  ```

- Installer les bibliothèques nécessaires :

  ```shell
  pip install -r requirements.txt
  ```

- Démarrer l'API :

  ```shell
  python api.py
  ```

- Démarrer la stack locale d'AWS :

  ```shell
  localstack start -d
  ./venv/bin/localstack status services
  ```

### Lancement du programme
/!\ Il faut que l'API soit lancée
Des documents de tests sont disponibles dans le dossier `documents/`
```
python main.py --file documents/test_1.pdf
```

### Commandes Utiles pour AWS en Local

- Créer une file SQS :

  ```shell
  ./venv/bin/awslocal sqs create-queue --queue-name sample-queue --region eu-west-3
  ```

- Créer une table DynamoDB :

  ```shell
  ./venv/bin/awslocal dynamodb create-table \
  --table-name TestTable \
  --attribute-definitions AttributeName=document,AttributeType=S \
  --key-schema AttributeName=document,KeyType=HASH \
  --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1 \
  --region eu-west-3
  ```

  Utilisation avec boto3 :

  ```python
  dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:4566', region_name='eu-west-3')
  ```

- Créer un groupe de logs et un flux de logs sur CloudWatch :

  ```shell
  ./venv/bin/awslocal logs create-log-group --log-group-name test-filter
  ./venv/bin/awslocal logs create-log-stream --log-group-name test-filter --log-stream-name test-filter-stream
  ```

- Créer un bucket S3 :

  ```shell
  ./venv/bin/awslocal s3api create-bucket --bucket test-bucket --create-bucket-configuration LocationConstraint=eu-west-3
  ```

  Utilisation avec boto3 :

  ```python
  s3 = boto3.client('s3', endpoint_url='http://test-bucket.s3.localhost.localstack.cloud:4566/', region_name='eu-west-3')
  ```

## Bonne chance !
