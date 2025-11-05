# Docker Hub GitHub Actions Setup

## Required GitHub Secrets

To enable automatic Docker image building and pushing to Docker Hub, you need to configure the following secrets in your GitHub repository:

### 1. **DOCKERHUB_USERNAME** (Required)
Your Docker Hub username.

### 2. **DOCKERHUB_TOKEN** (Required)
A Docker Hub access token (recommended) or password.

## How to Set Up the Secrets

### Step 1: Create a Docker Hub Access Token

1. Log in to [Docker Hub](https://hub.docker.com)
2. Go to **Account Settings** → **Security**
3. Click **New Access Token**
4. Give it a descriptive name (e.g., "GitHub Actions for rsqueue")
5. Select the appropriate permissions (Read & Write is typically needed)
6. Copy the generated token immediately (you won't be able to see it again)

### Step 2: Add Secrets to GitHub Repository

1. Go to your GitHub repository
2. Navigate to **Settings** → **Secrets and variables** → **Actions**
3. Click **New repository secret**
4. Add the following secrets:
   - **Name**: `DOCKERHUB_USERNAME`
     **Value**: Your Docker Hub username
   - **Name**: `DOCKERHUB_TOKEN`
     **Value**: The access token you created in Step 1

## Workflow Triggers

The workflows are configured to trigger on:

### docker-build-push.yml
- **Push to main branch**: Builds and pushes with `latest` and commit SHA tags
- **Pull requests**: Builds only (no push) for testing
- **Git tags** (v*): Builds and pushes with version tags
- **Manual trigger**: Via GitHub Actions UI (workflow_dispatch)

### docker-release.yml
- **GitHub Release published**: Builds and pushes with semantic version tags

## Image Tags

The workflows will create the following tags:

- `datamill/rsqueue:latest` - Always points to the latest main branch build
- `datamill/rsqueue:main` - Latest main branch build
- `datamill/rsqueue:v1.0.0` - Specific version (from git tags/releases)
- `datamill/rsqueue:1.0` - Major.minor version
- `datamill/rsqueue:1` - Major version only
- `datamill/rsqueue:main-abc1234` - Branch name with commit SHA

## Multi-platform Support

The workflows are configured to build for multiple platforms:
- `linux/amd64` (x86_64)
- `linux/arm64` (ARM 64-bit)
- `linux/arm/v7` (ARM 32-bit, in release workflow)

## Testing the Setup

1. Create a test branch and push a small change
2. Create a pull request to verify the build works (without pushing)
3. Merge to main to verify the push to Docker Hub works
4. Create a release/tag to verify version tagging works

## Troubleshooting

If the workflows fail:

1. **Authentication errors**: Verify your Docker Hub credentials are correct
2. **Permission errors**: Ensure the access token has Read & Write permissions
3. **Rate limiting**: Docker Hub has pull rate limits; consider using authentication even for pulls
4. **Build failures**: Check the Dockerfile and ensure it builds locally first

## Optional Enhancements

You can also add these optional secrets for enhanced functionality:

- **DOCKERHUB_DESCRIPTION**: Auto-update the Docker Hub repository description
- **COSIGN_PRIVATE_KEY**: For signing images with cosign
- **SLACK_WEBHOOK**: For build notifications