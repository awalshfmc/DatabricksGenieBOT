"""
Databricks Genie Bot

Authors: Luiz Carrossoni Neto, Ryan Bates
Revision: 1.1

This script implements an experimental chatbot that interacts with Databricks' Genie API. The bot facilitates conversations with Genie,
Databricks' AI assistant, through a chat interface.

Note: This is experimental code and is not intended for production use.


Update on May 02 to reflect Databricks API Changes https://www.databricks.com/blog/genie-conversation-apis-public-preview
Update on Aug 5 to reflect Microsoft Azure no longer supporting MultiTenant bots
"""

"""
Startup Command:
python3 -m aiohttp.web -H 0.0.0.0 -P 8000 app:init_func

"""

from asyncio.log import logger
import json
from typing import Optional
from aiohttp import web
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import GenieAPI
import asyncio
import traceback
from datetime import datetime, timezone
import uuid
from io import BytesIO
import pandas as pd
import requests
from aiohttp.web import Request, Response, json_response
from botbuilder.core import (
    BotFrameworkAdapterSettings,
    BotFrameworkAdapter,
    ActivityHandler,
    TurnContext,
)
from botbuilder.core.teams import TeamsActivityHandler
from botbuilder.schema.teams import FileConsentCard, FileInfoCard
from botbuilder.core.integration import aiohttp_error_middleware
from botbuilder.integration.aiohttp import (
    CloudAdapter,
    ConfigurationBotFrameworkAuthentication,
)
from botbuilder.schema import (
    Activity,
    Attachment,
    ConversationReference,
    ActivityTypes,
    ChannelAccount,
)

from config import DefaultConfig


CONFIG = DefaultConfig()

ADAPTER = CloudAdapter(ConfigurationBotFrameworkAuthentication(CONFIG))

# SETTINGS = BotFrameworkAdapterSettings(CONFIG.APP_ID, CONFIG.APP_PASSWORD,)
# ADAPTER = BotFrameworkAdapter(SETTINGS)

# Store query results temporarily for download
query_results_cache: dict[str, dict] = {}


async def on_error(context: TurnContext, error: Exception):
    # This check writes out errors to console log .vs. app insights.
    # NOTE: In production environment, you should consider logging this to Azure
    #       application insights.
    ##print(f"\n [on_turn_error] unhandled error: {error}", file=sys.stderr)
    traceback.print_exc()

    # Send a message to the user
    await context.send_activity("The bot encountered an error or bug.")
    await context.send_activity("To continue to run this bot, please fix the bot source code.")
    # Send a trace activity if we're talking to the Bot Framework Emulator
    if context.activity.channel_id == "emulator":
        # Create a trace activity that contains the error object
        trace_activity = Activity(
            label="TurnError",
            name="on_turn_error Trace",
            timestamp=datetime.now(timezone.utc),
            type=ActivityTypes.trace,
            value=f"{error}",
            value_type="https://www.botframework.com/schemas/error",
        )
        # Send a trace activity, which will be displayed in Bot Framework Emulator
        await context.send_activity(trace_activity)


ADAPTER.on_turn_error = on_error

workspace_client = WorkspaceClient(host=CONFIG.DATABRICKS_HOST, token=CONFIG.DATABRICKS_TOKEN)

genie_api = GenieAPI(workspace_client.api_client)


async def ask_genie(
    question: str, space_id: str, conversation_id: Optional[str] = None
) -> tuple[str, str]:
    try:
        loop = asyncio.get_running_loop()
        if conversation_id is None:
            initial_message = await loop.run_in_executor(
                None, genie_api.start_conversation_and_wait, space_id, question
            )
            conversation_id = initial_message.conversation_id
        else:

            initial_message = await loop.run_in_executor(
                None, genie_api.start_conversation_and_wait, space_id, question
            )

        query_result = None
        if initial_message.query_result is not None:
            query_result = await loop.run_in_executor(
                None,
                genie_api.get_message_attachment_query_result,
                # genie_api.get_message_query_result,
                space_id,
                initial_message.conversation_id,
                initial_message.message_id,
                initial_message.attachments[0].attachment_id,
            )
        message_content = await loop.run_in_executor(
            None,
            genie_api.get_message,
            space_id,
            initial_message.conversation_id,
            initial_message.message_id,
        )
        if query_result and query_result.statement_response:
            results = await loop.run_in_executor(
                None,
                workspace_client.statement_execution.get_statement,
                query_result.statement_response.statement_id,
            )

            query_description = ""
            for attachment in message_content.attachments:
                if attachment.query and attachment.query.description:
                    query_description = attachment.query.description
                    break

            return (
                json.dumps(
                    {
                        "columns": results.manifest.schema.as_dict(),
                        "data": results.result.as_dict(),
                        "query_description": query_description,
                    }
                ),
                conversation_id,
            )

        if message_content.attachments:
            for attachment in message_content.attachments:
                if attachment.text and attachment.text.content:
                    return (
                        json.dumps({"message": attachment.text.content}),
                        conversation_id,
                    )

        return json.dumps({"message": message_content.content}), conversation_id
    except Exception as e:
        logger.error(f"Error in ask_genie: {str(e)}")
        return (
            json.dumps({"error": "An error occurred while processing your request."}),
            conversation_id,
        )


def build_excel_bytes_from_answer(answer_json: dict) -> Optional[bytes]:
    try:
        columns = answer_json.get("columns", {})
        data = answer_json.get("data", {})
        if not (isinstance(columns, dict) and "columns" in columns and "data_array" in data):
            return None

        col_names = [c.get("name") for c in columns["columns"]]
        df = pd.DataFrame(data["data_array"], columns=col_names)

        out = BytesIO()
        with pd.ExcelWriter(out, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="Query Results", index=False)
            qd = answer_json.get("query_description")
            if qd:
                pd.DataFrame([{"Query Description": qd}]).to_excel(
                    writer, sheet_name="Description", index=False
                )
        out.seek(0)
        return out.read()
    except Exception:
        traceback.print_exc()
        return None


def process_query_results(answer_json: dict, user_id: Optional[str] = None) -> str:
    response = ""

    if answer_json.get("query_description"):
        response += f"## Query Description\n\n{answer_json['query_description']}\n\n"

    if "columns" in answer_json and "data" in answer_json:
        response += "## Query Results\n\n"
        columns = answer_json["columns"]
        data = answer_json["data"]

        if isinstance(columns, dict) and "columns" in columns and "data_array" in data:
            col_defs = columns["columns"]
            header = "| " + " | ".join(col["name"] for col in col_defs) + " |"
            separator = "|" + "|".join(["---" for _ in col_defs]) + "|"
            response += header + "\n" + separator + "\n"
            for row in data["data_array"]:
                cells = []
                for value, col in zip(row, col_defs):
                    t = (col.get("type_name") or "").upper()
                    if value is None:
                        cells.append("NULL")
                    elif t in {"DECIMAL", "DOUBLE", "FLOAT"}:
                        try: cells.append(f"{float(value):,.2f}")
                        except: cells.append(str(value))
                    elif t in {"INT", "BIGINT", "LONG"}:
                        try: cells.append(f"{int(value):,}")
                        except: cells.append(str(value))
                    else:
                        cells.append(str(value))
                response += "| " + " | ".join(cells) + " |\n"
        else:
            response += "Unexpected result format.\n\n"
    elif "message" in answer_json:
        response += f"{answer_json['message']}\n\n"
    else:
        response += "No data available.\n\n"

    return response


async def download_excel(request: Request) -> Response:
    download_id = request.match_info.get("download_id")

    if download_id not in query_results_cache:
        return Response(status=404, text="Download not found or expired")

    try:
        cached_data = query_results_cache[download_id]
        columns = cached_data["columns"]
        data = cached_data["data"]
        query_description = cached_data["query_description"]

        # Create DataFrame
        if isinstance(columns, dict) and "columns" in columns:
            column_names = [col["name"] for col in columns["columns"]]
            df = pd.DataFrame(data["data_array"], columns=column_names)

            # Create Excel file in memory
            output = BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                df.to_excel(writer, sheet_name="Query Results", index=False)

                # Add query description as a separate sheet if available
                if query_description:
                    desc_df = pd.DataFrame([{"Query Description": query_description}])
                    desc_df.to_excel(writer, sheet_name="Description", index=False)

            output.seek(0)

            # Generate filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"databricks_query_results_{timestamp}.xlsx"

            # Clean up cache entry
            del query_results_cache[download_id]

            return Response(
                body=output.read(),
                headers={
                    "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    "Content-Disposition": f'attachment; filename="{filename}"',
                },
            )
        else:
            return Response(status=400, text="Invalid data format")

    except Exception as e:
        logger.error(f"Error generating Excel file: {str(e)}")
        return Response(status=500, text="Error generating Excel file")


class MyBot(TeamsActivityHandler):
    def __init__(self):
        self.conversation_ids: dict[str, str] = {}
        self._excel_cache: dict[str, bytes] = {}

    async def on_message_activity(self, turn_context: TurnContext):
        question = turn_context.activity.text
        user_id = turn_context.activity.from_property.id
        conversation_id = self.conversation_ids.get(user_id)

        try:
            answer, new_conversation_id = await ask_genie(
                question, CONFIG.DATABRICKS_SPACE_ID, conversation_id
            )
            self.conversation_ids[user_id] = new_conversation_id

            answer_json = json.loads(answer)
            # 4a) send the normal text output
            text = process_query_results(answer_json, user_id=user_id)
            await turn_context.send_activity(text)

            # 4b) if we have tabular data, also offer the Excel as a Teams file card
            xbytes = build_excel_bytes_from_answer(answer_json)
            if xbytes:
                # cache the latest excel for this user (retrieved in on_invoke)
                self._excel_cache[user_id] = xbytes
                fname = f"databricks_query_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                consent = FileConsentCard(
                    description="Databricks Genie query results",
                    size_in_bytes=len(xbytes),
                    name=fname,
                )
                attachment = Attachment(
                    content_type="application/vnd.microsoft.teams.card.file.consent",
                    content=consent,
                    name=fname,
                )
                await turn_context.send_activity(Activity(attachments=[attachment]))

        except json.JSONDecodeError:
            await turn_context.send_activity("Failed to decode response from the server.")
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            await turn_context.send_activity("An error occurred while processing your request.")

    async def on_members_added_activity(
        self, members_added: list[ChannelAccount], turn_context: TurnContext
    ):
        ##print("Members added",members_added)
        for member in members_added:
            if member.id != turn_context.activity.recipient.id:
                await turn_context.send_activity("Welcome to the Databricks Genie Bot!")

    async def on_invoke_activity(self, turn_context: TurnContext):
        # Teams file-consent callbacks
        if turn_context.activity.name == "fileConsent/invoke":
            v = turn_context.activity.value or {}
            action = v.get("action")
            if action == "accept":
                info = v.get("uploadInfo", {}) or {}
                upload_url = info.get("uploadUrl")
                file_name = info.get("name")
                user_id = turn_context.activity.from_property.id

                xbytes = self._excel_cache.get(user_id)
                if not (upload_url and xbytes):
                    await turn_context.send_activity("Sorry, I couldn’t find the file to upload.")
                    return web.Response(status=200)

                # Upload to the pre-signed URL
                headers = {
                    "Content-Range": f"bytes 0-{len(xbytes)-1}/{len(xbytes)}",
                    "Content-Type": "application/octet-stream",
                }
                r = requests.put(upload_url, data=xbytes, headers=headers, timeout=60)
                r.raise_for_status()

                # Tell Teams we're done: send a File Info card (renders the preview tile)
                finfo = FileInfoCard(unique_id=info.get("uniqueId"), file_type="xlsx")
                attach = Attachment(
                    content_type="application/vnd.microsoft.teams.card.file.info",
                    name=file_name,
                    content=finfo,
                )
                await turn_context.send_activity(Activity(attachments=[attach]))
                return web.Response(status=200)

            elif action == "decline":
                await turn_context.send_activity("Okay—won’t upload the file.")
                return web.Response(status=200)

        # default handling
        return await super().on_invoke_activity(turn_context)


BOT = MyBot()


async def messages(req: Request) -> Response:
    if "application/json" in req.headers["Content-Type"]:
        body = await req.json()
    else:
        return Response(status=415)

    activity = Activity().deserialize(body)
    auth_header = req.headers.get("Authorization", "")

    try:
        response = await ADAPTER.process(req, BOT)
        ##print("Response from bot",response)
        if response:
            return json_response(data=response.body, status=response.status)
        return Response(status=201)
    except Exception as e:
        logger.error(f"Error processing request: {str(e)}")
        return Response(status=500)


async def health(request):
    return web.Response(text="ok")  # 200


async def robots(request):
    # Azure probes /robots933456.txt during warmup
    return web.Response(text="User-agent: *\nDisallow:", content_type="text/plain")


def init_func(argv):
    APP = web.Application(middlewares=[aiohttp_error_middleware])
    APP.router.add_get("/", health)
    APP.router.add_get("/robots933456.txt", robots)
    APP.router.add_post("/api/messages", messages)
    APP.router.add_get("/download/{download_id}", download_excel)
    return APP


if __name__ == "__main__":
    APP = init_func(None)
    try:
        HOST = "0.0.0.0"
        web.run_app(APP, host=HOST, port=CONFIG.PORT)
    except Exception as error:
        raise error
