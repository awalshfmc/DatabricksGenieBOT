#!/usr/bin/env python3
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import os
from dotenv import load_dotenv

load_dotenv()

""" Bot Configuration """


class DefaultConfig:
    """Bot Configuration"""

    PORT = 3978
    APP_ID = os.getenv("APP_ID")  # This is the application ID for the bot service.
    APP_PASSWORD = os.getenv("APP_PW")  # This is the password for the bot service.
    APP_TYPE = "SingleTenant"  # "SingleTenant" or "MultiTenant"
    APP_BASE_URL = os.getenv("APP_BASE_URL", "https://trade-data-genie.azurewebsites.net")  # This is the base URL for the bot service.
    # APP_TYPE = "MultiTenant" You can use this if testing locally
    APP_TENANTID = os.getenv("APP_TENANTID")  # This is the tenant ID for the bot service.
    DATABRICKS_SPACE_ID = os.getenv("GENIE_SPACE_ID")
    DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
    DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
