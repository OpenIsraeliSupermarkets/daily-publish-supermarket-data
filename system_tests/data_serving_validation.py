from typing import Dict, List, Optional, Any
from datetime import datetime
from utils.logging_config import Logger
from dataclasses import dataclass
from enum import Enum
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import os
import json
import multiprocessing
from functools import partial
from tqdm import tqdm
from tqdm.asyncio import tqdm as async_tqdm


@dataclass
class ValidationResult:
    num_of_rows: int
    status: str

def get_api_host():
    return "http://${API_IP}:8000/".format(API_IP=os.getenv("API_IP"))

class ApiCallValidator:
    def __init__(
        self, api_token: str, host: str = "http://localhost:8080", rate_limit: int = 10
    ):
        self.validation_results: Dict[str, Dict[str, ValidationResult]] = {}
        self.loading = False
        self.error: Optional[str] = None
        self.chains: List[str] = []
        self.selected_chain: Optional[str] = None
        self.files: List[str] = []
        self.selected_file: Optional[str] = None
        self.file_content: Optional[Dict[str, Any]] = None
        self.sort_config = {"key": "", "direction": "ascending"}
        self.api_token = api_token
        self.host = host.rstrip("/")  # Remove trailing slash if present
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_token}",
        }
        self.semaphore = asyncio.Semaphore(rate_limit)  # Add rate limiting semaphore

    async def check_health(self) -> Dict[str, Any]:
        """Check the health of the API service"""
        async with self.semaphore:
            async with aiohttp.ClientSession(headers=self.headers) as session:
                async with session.get(f"{self.host}/service_health") as response:
                    if response.status != 200:
                        raise Exception(f"Health check failed: {response.status}")
                    return await response.json()

    async def check_short_term_health(self) -> Dict[str, Any]:
        """Check the health of the short-term database"""
        async with self.semaphore:
            async with aiohttp.ClientSession(headers=self.headers) as session:
                async with session.get(f"{self.host}/short_term_health") as response:
                    if response.status != 200:
                        raise Exception(
                            f"Short-term health check failed: {response.status}"
                        )
                    return await response.json()

    async def check_long_term_health(self) -> Dict[str, Any]:
        """Check the health of the long-term database"""
        async with self.semaphore:
            async with aiohttp.ClientSession(headers=self.headers) as session:
                async with session.get(f"{self.host}/long_term_health") as response:
                    if response.status != 200:
                        raise Exception(
                            f"Long-term health check failed: {response.status}"
                        )
                    return await response.json()

    async def fetch_chains(self) -> List[str]:
        """Fetch all available chains from the API"""
        async with self.semaphore:  # Add rate limiting
            async with aiohttp.ClientSession(headers=self.headers) as session:
                async with session.get(f"{self.host}/list_chains") as response:
                    if response.status != 200:
                        raise Exception(f"Failed to fetch chains: {response.status}")
                    data = await response.json()
                    return data.get("list_of_chains", [])

    async def fetch_files_for_chain(self, chain: str) -> List[str]:
        """Fetch all files for a specific chain"""
        async with self.semaphore:  # Add rate limiting
            async with aiohttp.ClientSession(headers=self.headers) as session:
                async with session.get(
                    f"{self.host}/list_scraped_files?chain={chain}"
                ) as response:
                    if response.status != 200:
                        raise Exception(
                            f"Failed to fetch files for chain {chain}: {response.status}"
                        )
                    data = await response.json()
                    return [
                        file["file_name"] for file in data.get("processed_files", [])
                    ]

    async def fetch_file_content(self, chain: str, file: str) -> Dict[str, Any]:
        """Fetch content for a specific file"""
        async with self.semaphore:  # Add rate limiting
            async with aiohttp.ClientSession(headers=self.headers) as session:
                async with session.get(
                    f"{self.host}/raw/file_content?chain={chain}&file={file}"
                ) as response:
                    if response.status != 200:
                        raise Exception(
                            f"Failed to fetch content for {chain}/{file}: {response.status}, {response.reason}"
                        )
                    return (await response.json()).get("rows", [])

    async def validate_all_data(self) -> Dict[str, Dict[str, Any]]:
        """Fetch and validate all chains, files, and their content"""
        try:
            # Fetch all chains
            chains = await self.fetch_chains()

            # Process chains sequentially with progress bar
            results = {}
            for chain in tqdm(chains, desc="Processing chains"):
                results[chain] = await self._process_chain(chain)

            return results

        except Exception as e:
            Logger.error(f"Error in validate_all_data: {str(e)}")
            raise

    async def _process_chain(self, chain: str) -> Dict[str, Any]:
        """Process a single chain and its files"""
        result = {"files": [], "validation_results": []}

        try:
            # Fetch files for the chain
            files = await self.fetch_files_for_chain(chain)
            result["files"] = files

            # Process files concurrently with progress bar
            file_tasks = []
            for file in tqdm(files, desc=f"Processing files for {chain}", leave=False):
                file_tasks.append(
                    await asyncio.create_task(self._process_file(chain, file))
                )

            # Wait for all file processing to complete
            # file_results = await asyncio.gather(*file_tasks)
            result["validation_results"] = file_tasks

        except Exception as e:
            Logger.error(f"Error processing chain {chain}: {str(e)}")
            result["validation_results"] = [
                {
                    "file": "chain_error",
                    "validation": {"api_call_status": "error", "error": str(e)},
                }
            ]

        return result

    async def _process_file(self, chain: str, file: str) -> Dict[str, Any]:
        """Process a single file"""
        try:
            # Fetch file content
            content = await self.fetch_file_content(chain, file)

            # Validate the content
            validation_result = self._validate_content(content)

            return {"file": file, "validation": validation_result}

        except Exception as e:
            Logger.error(f"Error processing file {file} for chain {chain}: {str(e)}")
            return {
                "file": file,
                "validation": {"api_call_status": "error", "error": str(e)},
            }

    def _validate_content(self, content: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Validate content in a separate process"""
        return {"num_of_rows": len(content), "api_call_status": "success"}


async def main(api_token, host, rate_limit):

    if not api_token:
        raise ValueError("API_TOKEN environment variable is not set")

    Logger.info(f"Using API host: {host}")

    # Initialize validator with host and rate limit
    validator = ApiCallValidator(api_token, host, rate_limit)

    try:
        Logger.info("Starting data validation process...")

        # Check service health
        Logger.info("Checking service health...")
        health_result = await validator.check_health()
        Logger.info(f"Service health: {health_result['status']}")
        if health_result["status"] != "healthy":
            raise Exception(f"Service health is not healthy: {health_result['status']}")

        # Check short-term database health
        Logger.info("Checking short-term database health...")
        short_term_health = await validator.check_short_term_health()
        if not short_term_health["is_updated"]:
            raise Exception(
                f"Short-term database is not updated: {short_term_health['last_update']}"
            )

        # Check long-term database health
        Logger.info("Checking long-term database health...")
        long_term_health = await validator.check_long_term_health()
        if not long_term_health["is_updated"]:
            raise Exception(
                f"Long-term database is not updated: {long_term_health['last_update']}"
            )

        # Run validation
        results = await validator.validate_all_data()

        # Save results to file
        output_file = "validation_results.json"
        with open(output_file, "w") as f:
            json.dump(results, f, indent=2, default=str)
        Logger.info(f"\nResults saved to {output_file}")

        for chain in results:
            failed_files = list(
                filter(
                    lambda x: x["validation"]["api_call_status"] != "success",
                    results[chain]["validation_results"],
                )
            )
            if failed_files:
                Logger.error(f"Failed files for chain {chain}: {failed_files}")
                raise Exception(f"Failed files for chain {chain}: {failed_files}")
            else:
                Logger.info(f"No failed files for chain {chain}")
    except Exception as e:
        Logger.error(f"Error in main process: {str(e)}")
        raise
    finally:
        Logger.info("Validation process completed")


if __name__ == "__main__":
    # Get API token and host from environment variables
    api_token = os.getenv("API_TOKEN")
    host = os.getenv("API_HOST")
    rate_limit = int(os.getenv("RATE_LIMIT", "3"))  # Add rate limit env var
    asyncio.run(main(api_token, get_api_host(), rate_limit))
