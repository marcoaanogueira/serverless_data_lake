# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
import boto3
from pathlib import Path

from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.utilities.parser import event_parser

import xtable
from models import ConversionTask


# clients
lambda_client = boto3.client("lambda")


@event_parser(model=ConversionTask)
def handler(event: ConversionTask, context: LambdaContext):

    xtable.sync(
        dataset_config=event.dataset_config,
        tmp_path=Path("/tmp"),
        jars=[
            Path(__file__).resolve().parent / "jars/*",
        ],
    )
