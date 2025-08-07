#!/usr/bin/env python3
"""
Startup script for HTTP MCP Snowflake Server
"""

import argparse
import json
import logging
import os
import sys
import tomli
from typing import Dict, Any

# Add the src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from http_server import initialize_server, run_server

logger = logging.getLogger("mcp_snowflake_startup")


def load_toml_config(connections_file: str, connection_name: str) -> Dict[str, Any]:
    """Load Snowflake connection configuration from TOML file"""
    try:
        with open(connections_file, "rb") as f:
            config = tomli.load(f)
        
        if connection_name not in config:
            raise ValueError(f"Connection '{connection_name}' not found in {connections_file}")
        
        return config[connection_name]
    except Exception as e:
        logger.error(f"Error loading TOML configuration: {e}")
        raise


def load_runtime_config(config_file: str = "runtime_config.json") -> Dict[str, Any]:
    """Load runtime configuration"""
    try:
        if os.path.exists(config_file):
            with open(config_file, "r") as f:
                return json.load(f)
        return {}
    except Exception as e:
        logger.warning(f"Could not load runtime config: {e}")
        return {}


def main():
    parser = argparse.ArgumentParser(description="HTTP MCP Snowflake Server")
    parser.add_argument("--connections-file", required=True, help="Path to TOML connections file")
    parser.add_argument("--connection-name", required=True, help="Name of connection to use")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind to (default: 8000)")
    parser.add_argument("--allow-write", action="store_true", help="Allow write operations")
    parser.add_argument("--exclude-tools", nargs="*", default=[], help="Tools to exclude")
    parser.add_argument("--exclude-json-results", action="store_true", help="Exclude JSON results")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    parser.add_argument("--config-file", default="runtime_config.json", help="Runtime config file")
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    try:
        # Load Snowflake connection configuration
        connection_args = load_toml_config(args.connections_file, args.connection_name)
        logger.info(f"Loaded connection config for '{args.connection_name}'")
        
        # Load runtime configuration
        runtime_config = load_runtime_config(args.config_file)
        exclusion_config = runtime_config.get("exclude_patterns", {
            "databases": ["temp"], 
            "schemas": ["temp", "information_schema"], 
            "tables": ["temp"]
        })
        
        # Initialize the MCP server
        initialize_server(
            connection_args=connection_args,
            allow_write=args.allow_write,
            exclude_tools=args.exclude_tools,
            exclude_json_results=args.exclude_json_results,
            exclusion_config=exclusion_config
        )
        
        logger.info(f"Starting HTTP MCP server on {args.host}:{args.port}")
        logger.info(f"Write operations: {'enabled' if args.allow_write else 'disabled'}")
        logger.info(f"Excluded tools: {args.exclude_tools}")
        
        # Print the URL for Claude Desktop
        print("\n" + "="*60)
        print("🚀 MCP Snowflake HTTP Server Starting")
        print("="*60)
        print(f"Server URL: http://{args.host}:{args.port}")
        print(f"Tools endpoint: http://{args.host}:{args.port}/tools")
        print(f"Health check: http://{args.host}:{args.port}/")
        print("\nFor Claude Desktop, use this URL:")
        print(f"http://{args.host}:{args.port}")
        print("="*60 + "\n")
        
        # Start the server
        run_server(host=args.host, port=args.port)
        
    except Exception as e:
        logger.error(f"Failed to start server: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
