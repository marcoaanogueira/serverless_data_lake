#!/bin/bash
# deploy.sh — Full deploy: CDK + frontend build + S3 upload
#
# Usage:
#   ./scripts/deploy.sh
#
# What it does:
#   1. cdk deploy
#   2. Reads the API Gateway URL from the stack output
#   3. Runs npm run build with VITE_API_URL set
#   4. Uploads dist/ to S3 and invalidates CloudFront

set -e

STACK_NAME="ServerlessDataLakeStack"

echo "==> Deploying CDK stack..."
cdk deploy --require-approval broadening

echo ""
echo "==> Reading stack outputs..."
API_URL=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --query 'Stacks[0].Outputs[?OutputKey==`ApiEndpoint`].OutputValue' \
  --output text)

BUCKET=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --query 'Stacks[0].Outputs[?contains(OutputKey,`BucketName`)].OutputValue' \
  --output text)

DIST_ID=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --query 'Stacks[0].Outputs[?contains(OutputKey,`DistributionId`)].OutputValue' \
  --output text)

WEBSITE_URL=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --query 'Stacks[0].Outputs[?contains(OutputKey,`WebsiteURL`)].OutputValue' \
  --output text)

echo "    API URL:     $API_URL"
echo "    S3 Bucket:   $BUCKET"
echo "    CloudFront:  $DIST_ID"

echo ""
echo "==> Building frontend with VITE_API_URL=$API_URL..."
cd frontend
VITE_API_URL="$API_URL" npm run build
cd ..

echo ""
echo "==> Uploading to S3..."
aws s3 sync frontend/dist "s3://$BUCKET" --delete --quiet

echo ""
echo "==> Invalidating CloudFront cache..."
aws cloudfront create-invalidation \
  --distribution-id "$DIST_ID" \
  --paths "/*" \
  --output text > /dev/null

echo ""
echo "✅ Done! Website: $WEBSITE_URL"
