import aws_cdk as core
import aws_cdk.assertions as assertions

from serverless data lake.serverless data lake_stack import ServerlessDataLakeStack

# example tests. To run these tests, uncomment this file along with the example
# resource in serverless data lake/serverless data lake_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = ServerlessDataLakeStack(app, "serverless-data-lake")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
