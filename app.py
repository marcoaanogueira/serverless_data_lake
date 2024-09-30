#!/usr/bin/env python3
import os
import aws_cdk as cdk
from stack.serverless_data_lake_stack import ServerlessDataLakeStack


app = cdk.App()
ServerlessDataLakeStack(
    app,
    "ServerlessDataLakeStack",
    env=cdk.Environment(
        account=os.getenv("CDK_DEFAULT_ACCOUNT"), region=os.getenv("CDK_DEFAULT_REGION")
    ),
)

app.synth()
