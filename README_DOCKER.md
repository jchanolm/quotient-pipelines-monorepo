# Dockerized Farcaster Scraper

This guide explains how to run the Farcaster data scraper in a Docker container for cloud deployment.

## Prerequisites

- Docker and Docker Compose installed
- Neynar API key
- Farcaster mnemonic
- Neo4j database access
- AWS S3 credentials (optional, for data storage)

## Setup

1. Clone this repository
2. Create a `.env` file from the example:
   ```
   cp .env.example .env
   ```
3. Edit the `.env` file with your actual credentials and configuration

## Running Locally with Docker

```bash
# Build and start the container
docker-compose up --build

# Run in the background
docker-compose up -d

# View logs
docker-compose logs -f

# Stop the container
docker-compose down
```

## Deploying to Cloud Services

### AWS ECS (Elastic Container Service)

1. Create an ECR repository
2. Build and push the Docker image:
   ```bash
   aws ecr get-login-password --region your-region | docker login --username AWS --password-stdin your-account-id.dkr.ecr.your-region.amazonaws.com
   docker build -t your-account-id.dkr.ecr.your-region.amazonaws.com/farcaster-scraper:latest .
   docker push your-account-id.dkr.ecr.your-region.amazonaws.com/farcaster-scraper:latest
   ```
3. Create an ECS Task Definition with your environment variables
4. Create an ECS Cluster and Service to run the task

### Google Cloud Run

1. Build and push the Docker image:
   ```bash
   gcloud builds submit --tag gcr.io/your-project/farcaster-scraper
   ```
2. Deploy to Cloud Run:
   ```bash
   gcloud run deploy farcaster-scraper \
     --image gcr.io/your-project/farcaster-scraper \
     --platform managed \
     --set-env-vars "NEYNAR_API_KEY=your-key,..."
   ```

### Azure Container Instances

1. Create an Azure Container Registry
2. Build and push the Docker image:
   ```bash
   az acr build --registry YourRegistry --image farcaster-scraper:latest .
   ```
3. Create a Container Instance with your environment variables

## Configuration Options

The scraper accepts the following environment variables:

- `NEYNAR_API_KEY`: Your Neynar API key
- `MNEMONIC`: Your Farcaster wallet mnemonic
- `NEO4J_URI`: Neo4j database URI
- `NEO4J_USERNAME`: Neo4j username
- `NEO4J_PASSWORD`: Neo4j password
- `AWS_ACCESS_KEY_ID`: AWS access key ID
- `AWS_SECRET_ACCESS_KEY`: AWS secret access key
- `S3_BUCKET_NAME`: S3 bucket name for data storage

## Running on a Schedule

For cloud services, you can set up scheduled execution:

- AWS: Use EventBridge to trigger the ECS task on a schedule
- Google Cloud: Use Cloud Scheduler to trigger Cloud Run
- Azure: Use Logic Apps or Azure Functions to trigger the container

## Troubleshooting

- Check container logs: `docker-compose logs -f`
- Verify environment variables are properly set
- Ensure the container has network access to Neo4j and external APIs
- Check rate limiting if API calls are failing 