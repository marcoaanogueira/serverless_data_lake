import aws_cdk as core
import aws_cdk.assertions as assertions
from stack.serverless_data_lake_stack import ServerlessDataLakeStack

app = core.App()
stack = ServerlessDataLakeStack(app, "serverless-data-lake")
template = assertions.Template.from_stack(stack)


def test_buckets_created():
    # Verificar se os buckets Bronze, Silver, Gold e Artifacts foram criados
    bucket_names = ["bronze", "silver", "gold", "artifacts"]
    for bucket in bucket_names:
        template.has_resource_properties(
            "AWS::S3::Bucket",
            {"BucketName": assertions.Match.string_like_regexp(f"decolares-{bucket}")},
        )


def test_lambda_function_created_with_ecr():
    # Verificar se a Lambda foi criada com DockerImageCode (ECR)
    template.has_resource_properties("AWS::Lambda::Function", {"PackageType": "Image"})


def test_lambda_function_created_with_layers():
    # Verificar se a Lambda foi criada com layers quando `use_ecr=False`
    template.has_resource_properties(
        "AWS::Lambda::Function",
        {"Runtime": "python3.10", "Layers": assertions.Match.any_value()},
    )


def test_firehose_created_for_tables():
    # Verificar se o Firehose foi criado com destino no bucket S3
    template.has_resource_properties(
        "AWS::KinesisFirehose::DeliveryStream",
        {
            "S3DestinationConfiguration": {
                "BucketARN": assertions.Match.any_value(),
                "RoleARN": assertions.Match.any_value(),
            }
        },
    )


def test_lambda_has_firehose_permissions():
    # Verificar se a Lambda tem permisssões para usar `firehose:PutRecord` e `firehose:PutRecordBatch`
    template.has_resource_properties(
        "AWS::IAM::Policy",
        {
            "PolicyDocument": {
                "Statement": assertions.Match.array_with(
                    [
                        {
                            "Action": ["firehose:PutRecord", "firehose:PutRecordBatch"],
                            "Effect": "Allow",
                            # Adicionando verificação para o Resource
                            "Resource": {
                                "Fn::GetAtt": ["DecolaresvendasFirehose", "Arn"]
                            },
                        }
                    ]
                )
            }
        },
    )
