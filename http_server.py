#!/usr/bin/env python3
"""
HTTP-enabled MCP Snowflake Server
Exposes the Snowflake MCP server over HTTP with SSE transport for Claude Desktop integration
"""

import asyncio
import json
import logging
import os
import sys
from typing import Any, Dict, Optional

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles

# Import our existing MCP server components
from src.mcp_snowflake_server.db_client import SnowflakeDB
from src.mcp_snowflake_server.write_detector import SQLWriteDetector
from src.mcp_snowflake_server.server import (
    handle_list_databases,
    handle_list_schemas, 
    handle_list_tables,
    handle_describe_table,
    handle_read_query,
    handle_append_insight,
    handle_write_query,
    handle_create_table,
    Tool
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("mcp_snowflake_http_server")

app = FastAPI(
    title="Snowflake MCP Server",
    description="Model Context Protocol server for Snowflake database operations",
    version="1.0.0"
)

# Enable CORS for Claude Desktop
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global variables for our MCP components
db: Optional[SnowflakeDB] = None
write_detector: Optional[SQLWriteDetector] = None
config: Dict[str, Any] = {}
allowed_tools: list[Tool] = []


class MCPHTTPServer:
    def __init__(
        self,
        connection_args: dict,
        allow_write: bool = False,
        exclude_tools: list[str] = None,
        exclude_json_results: bool = False,
        exclusion_config: dict = None
    ):
        self.connection_args = connection_args
        self.allow_write = allow_write
        self.exclude_tools = exclude_tools or []
        self.exclude_json_results = exclude_json_results
        self.exclusion_config = exclusion_config or {"databases": [], "schemas": [], "tables": []}
        
        # Initialize database connection
        self.db = SnowflakeDB(connection_args)
        self.db.start_init_connection()
        
        self.write_detector = SQLWriteDetector()
        
        # Define all available tools
        self.all_tools = [
            Tool(
                name="list_databases",
                description="List all available databases in Snowflake",
                input_schema={"type": "object", "properties": {}},
                handler=handle_list_databases,
            ),
            Tool(
                name="list_schemas",
                description="List all schemas in a database",
                input_schema={
                    "type": "object",
                    "properties": {
                        "database": {
                            "type": "string",
                            "description": "Database name to list schemas from",
                        },
                    },
                    "required": ["database"],
                },
                handler=handle_list_schemas,
            ),
            Tool(
                name="list_tables",
                description="List all tables in a specific database and schema",
                input_schema={
                    "type": "object",
                    "properties": {
                        "database": {"type": "string", "description": "Database name"},
                        "schema": {"type": "string", "description": "Schema name"},
                    },
                    "required": ["database", "schema"],
                },
                handler=handle_list_tables,
            ),
            Tool(
                name="describe_table",
                description="Get the schema information for a specific table",
                input_schema={
                    "type": "object",
                    "properties": {
                        "table_name": {
                            "type": "string",
                            "description": "Fully qualified table name in the format 'database.schema.table'",
                        },
                    },
                    "required": ["table_name"],
                },
                handler=handle_describe_table,
            ),
            Tool(
                name="read_query",
                description="Execute a SELECT query",
                input_schema={
                    "type": "object",
                    "properties": {"query": {"type": "string", "description": "SELECT SQL query to execute"}},
                    "required": ["query"],
                },
                handler=handle_read_query,
            ),
            Tool(
                name="append_insight",
                description="Add a data insight to the memo",
                input_schema={
                    "type": "object",
                    "properties": {
                        "insight": {
                            "type": "string",
                            "description": "Data insight discovered from analysis",
                        }
                    },
                    "required": ["insight"],
                },
                handler=handle_append_insight,
                tags=["resource_based"],
            ),
        ]
        
        # Add write tools if allowed
        if allow_write:
            self.all_tools.extend([
                Tool(
                    name="write_query",
                    description="Execute an INSERT, UPDATE, or DELETE query on the Snowflake database",
                    input_schema={
                        "type": "object",
                        "properties": {"query": {"type": "string", "description": "SQL query to execute"}},
                        "required": ["query"],
                    },
                    handler=handle_write_query,
                    tags=["write"],
                ),
                Tool(
                    name="create_table",
                    description="Create a new table in the Snowflake database",
                    input_schema={
                        "type": "object",
                        "properties": {"query": {"type": "string", "description": "CREATE TABLE SQL statement"}},
                        "required": ["query"],
                    },
                    handler=handle_create_table,
                    tags=["write"],
                ),
            ])
        
        # Filter excluded tools
        exclude_tags = [] if allow_write else ["write"]
        self.allowed_tools = [
            tool for tool in self.all_tools 
            if tool.name not in self.exclude_tools and not any(tag in exclude_tags for tag in tool.tags)
        ]
        
        logger.info(f"Initialized MCP HTTP server with tools: {[tool.name for tool in self.allowed_tools]}")

    async def call_tool(self, name: str, arguments: dict = None) -> dict:
        """Execute a tool and return the result"""
        if name in self.exclude_tools:
            return {"error": f"Tool {name} is excluded from this data connection"}
        
        handler = next((tool.handler for tool in self.allowed_tools if tool.name == name), None)
        if not handler:
            return {"error": f"Unknown tool: {name}"}
        
        try:
            # Pass appropriate parameters based on tool type
            if name in ["list_databases", "list_schemas", "list_tables"]:
                result = await handler(
                    arguments,
                    self.db,
                    self.write_detector,
                    self.allow_write,
                    None,  # server object not needed for HTTP
                    exclusion_config=self.exclusion_config,
                    exclude_json_results=self.exclude_json_results,
                )
            else:
                result = await handler(
                    arguments,
                    self.db,
                    self.write_detector,
                    self.allow_write,
                    None,  # server object not needed for HTTP
                    exclude_json_results=self.exclude_json_results,
                )
            
            # Convert result to JSON-serializable format
            if isinstance(result, list):
                # Extract text content from MCP response types
                return {
                    "result": [
                        item.text if hasattr(item, 'text') else str(item)
                        for item in result
                    ]
                }
            else:
                return {"result": str(result)}
                
        except Exception as e:
            logger.error(f"Error executing tool {name}: {str(e)}")
            return {"error": str(e)}

    def get_tools_list(self) -> list[dict]:
        """Return list of available tools"""
        return [
            {
                "name": tool.name,
                "description": tool.description,
                "input_schema": tool.input_schema,
            }
            for tool in self.allowed_tools
        ]


# Global server instance
mcp_server: Optional[MCPHTTPServer] = None


@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "name": "Snowflake MCP Server",
        "version": "1.0.0",
        "status": "running",
        "transport": "http"
    }


@app.get("/tools")
async def list_tools():
    """List all available tools"""
    if not mcp_server:
        raise HTTPException(status_code=500, detail="Server not initialized")
    return {"tools": mcp_server.get_tools_list()}


@app.post("/tools/{tool_name}")
async def call_tool(tool_name: str, arguments: dict = None):
    """Execute a specific tool"""
    if not mcp_server:
        raise HTTPException(status_code=500, detail="Server not initialized")
    
    result = await mcp_server.call_tool(tool_name, arguments)
    return result


@app.get("/sse")
async def sse_endpoint():
    """Server-Sent Events endpoint for real-time communication"""
    async def event_stream():
        # Send initial connection message
        yield f"data: {json.dumps({'type': 'connection', 'status': 'connected'})}\n\n"
        
        # Keep connection alive
        while True:
            yield f"data: {json.dumps({'type': 'ping', 'timestamp': asyncio.get_event_loop().time()})}\n\n"
            await asyncio.sleep(30)  # Ping every 30 seconds
    
    return StreamingResponse(event_stream(), media_type="text/plain")


def initialize_server(
    connection_args: dict,
    allow_write: bool = False,
    exclude_tools: list[str] = None,
    exclude_json_results: bool = False,
    exclusion_config: dict = None
):
    """Initialize the global MCP server instance"""
    global mcp_server
    mcp_server = MCPHTTPServer(
        connection_args=connection_args,
        allow_write=allow_write,
        exclude_tools=exclude_tools or [],
        exclude_json_results=exclude_json_results,
        exclusion_config=exclusion_config or {"databases": [], "schemas": [], "tables": []}
    )
    logger.info("MCP HTTP Server initialized successfully")


def run_server(host: str = "0.0.0.0", port: int = 8000, **kwargs):
    """Run the HTTP server"""
    logger.info(f"Starting HTTP server on {host}:{port}")
    uvicorn.run(app, host=host, port=port, log_level="info")


if __name__ == "__main__":
    # This will be used when we need to start the server
    print("HTTP MCP Server module loaded. Use initialize_server() and run_server() to start.")
