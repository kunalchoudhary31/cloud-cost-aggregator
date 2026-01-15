"""
Azure Cost collector
Supports both:
1. Paid Azure accounts via Cost Management API (using service principal)
2. Azure Sponsorship accounts via cookie-based portal API
"""
from datetime import date, timedelta
from typing import List, Dict, Optional
from decimal import Decimal
import requests
import json
from json import JSONDecodeError

from collectors.base_collector import BaseCollector, CostRecord
from config import AzureConfig


class AzureCollector(BaseCollector):
    """
    Collector for Azure costs
    Supports both paid accounts (Cost Management API) and Sponsorship accounts (cookie-based)
    """

    def __init__(self, config: AzureConfig):
        """
        Initialize Azure collector

        Args:
            config: Azure configuration
        """
        super().__init__('azure')
        self.config = config
        
        # Determine which method to use
        self.use_cost_management_api = self._should_use_cost_management_api()
        
        if self.use_cost_management_api:
            self.logger.info("Using Azure Cost Management API (paid account)")
            self._init_cost_management_client()
        else:
            self.logger.info("Using Azure Sponsorship portal API (cookie-based)")
            self.api_url = "https://www.microsoftazuresponsorships.com/Usage/GetSubscriptionData"
    
    def _should_use_cost_management_api(self) -> bool:
        """
        Determine if we should use Cost Management API (paid account) or Sponsorship API
        
        Returns:
            True if service principal credentials are available (paid account)
            False if only cookies are available (sponsorship account)
        """
        has_service_principal = (
            self.config.tenant_id and 
            self.config.client_id and 
            self.config.client_secret and 
            self.config.subscription_id
        )
        
        has_cookies = bool(self.config.sponsorship_cookies)
        
        if has_service_principal:
            self.logger.info("Service principal credentials detected - using Cost Management API")
            return True
        elif has_cookies:
            self.logger.info("Only cookies detected - using Sponsorship portal API")
            return False
        else:
            self.logger.warning("No valid credentials found. Will attempt Cost Management API first.")
            return True
    
    def _init_cost_management_client(self):
        """Initialize Azure Cost Management API client"""
        try:
            from azure.identity import ClientSecretCredential
            from azure.mgmt.costmanagement import CostManagementClient
            from azure.mgmt.costmanagement.models import QueryDefinition, QueryTimePeriod
            
            self.credential = ClientSecretCredential(
                tenant_id=self.config.tenant_id,
                client_id=self.config.client_id,
                client_secret=self.config.client_secret
            )
            
            self.cost_client = CostManagementClient(self.credential)
            self.logger.info("Azure Cost Management client initialized successfully")
        except ImportError:
            self.logger.error(
                "Azure SDK packages not installed. Install with: "
                "pip install azure-identity azure-mgmt-costmanagement"
            )
            raise
        except Exception as e:
            self.logger.error(f"Failed to initialize Azure Cost Management client: {e}")
            raise

    def test_connection(self) -> bool:
        """
        Test Azure API connection

        Returns:
            True if connection successful, False otherwise
        """
        if self.use_cost_management_api:
            return self._test_cost_management_api()
        else:
            return self._test_sponsorship_api()
    
    def _test_cost_management_api(self) -> bool:
        """Test Cost Management API connection"""
        try:
            if not all([self.config.tenant_id, self.config.client_id, 
                       self.config.client_secret, self.config.subscription_id]):
                self.logger.error("Azure Cost Management API requires: AZURE_TENANT_ID, "
                                "AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_SUBSCRIPTION_ID")
                return False
            
            self.logger.info("Testing Azure Cost Management API connection...")
            
            # Test with a small date range
            test_start = date.today() - timedelta(days=7)
            test_end = date.today()
            
            from azure.mgmt.costmanagement.models import (
                QueryDefinition, 
                QueryTimePeriod, 
                QueryDataset, 
                QueryAggregation
            )
            from datetime import datetime, timezone
            
            # Convert dates to ISO format strings (matching sample-cost.py)
            from_datetime = datetime.combine(test_start, datetime.min.time()).replace(tzinfo=timezone.utc)
            to_datetime = datetime.combine(test_end, datetime.max.time()).replace(tzinfo=timezone.utc)
            from_iso = from_datetime.isoformat()
            to_iso = to_datetime.isoformat()
            
            # Create proper QueryAggregation object
            aggregation = QueryAggregation(
                name="PreTaxCost",
                function="Sum"
            )
            
            query_definition = QueryDefinition(
                type="Usage",  # Changed from "ActualCost" to "Usage" (matching sample-cost.py)
                timeframe="Custom",
                time_period=QueryTimePeriod(
                    from_property=from_iso,  # Using ISO string format
                    to=to_iso  # Using ISO string format
                ),
                dataset=QueryDataset(
                    granularity="Daily",  # Keeping "Daily" for daily breakdown
                    aggregation={
                        "totalCost": aggregation
                    }
                )
            )
            
            scope = f"/subscriptions/{self.config.subscription_id}"
            result = self.cost_client.query.usage(scope=scope, parameters=query_definition)
            
            self.logger.info("Azure Cost Management API connection test successful")
            return True
            
        except Exception as e:
            self.logger.error(f"Azure Cost Management API connection test failed: {e}")
            return False
    
    def _test_sponsorship_api(self) -> bool:
        """Test Sponsorship portal API connection"""
        try:
            if not self.config.subscription_id:
                self.logger.error("Azure connection test failed: AZURE_SUBSCRIPTION_ID is not set")
                return False
            
            if not self.config.sponsorship_cookies:
                self.logger.error("Azure connection test failed: AZURE_SPONSORSHIP_COOKIES is not set")
                return False

            self.logger.info("Testing Azure Sponsorship API connection...")
            self.logger.debug(f"Using subscription ID: {self.config.subscription_id[:8]}...")
            
            # Test with a small date range
            test_start = date.today() - timedelta(days=7)
            test_end = date.today()

            params = {
                'startDate': test_start.strftime('%Y-%m-%d'),
                'endDate': test_end.strftime('%Y-%m-%d'),
                'subscriptionGuid': self.config.subscription_id
            }

            self.logger.debug(f"Making request to {self.api_url} with params: {params}")
            headers = self._get_headers()
            
            self.logger.info("Sending HTTP request (timeout: 30s)...")
            response = requests.get(self.api_url, params=params, headers=headers, timeout=30)
            self.logger.info(f"Received response with status code: {response.status_code}")

            if response.status_code == 200:
                self.logger.info("Azure Sponsorship API connection test successful")
                return True
            else:
                self.logger.error(f"Azure connection test failed: HTTP {response.status_code}")
                if response.text:
                    self.logger.debug(f"Response body: {response.text[:200]}")
                return False

        except requests.exceptions.Timeout:
            self.logger.error("Azure connection test failed: Request timed out after 30 seconds")
            return False
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Azure connection test failed: Network error - {e}")
            return False
        except Exception as e:
            self.logger.error(f"Azure connection test failed: {e}", exc_info=True)
            return False

    def collect_costs(
        self,
        start_date: date,
        end_date: date
    ) -> List[CostRecord]:
        """
        Collect Azure costs for the specified date range

        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)

        Returns:
            List of CostRecord objects
        """
        if self.use_cost_management_api:
            return self._collect_costs_via_api(start_date, end_date)
        else:
            return self._collect_costs_via_sponsorship(start_date, end_date)
    
    def _collect_costs_via_api(
        self,
        start_date: date,
        end_date: date
    ) -> List[CostRecord]:
        """Collect costs using Azure Cost Management API"""
        self.logger.info(f"Collecting Azure costs via Cost Management API from {start_date} to {end_date}")
        
        try:
            from azure.mgmt.costmanagement.models import (
                QueryDefinition, 
                QueryTimePeriod, 
                QueryDataset,
                QueryAggregation,
                QueryGrouping
            )
            from datetime import datetime, timezone
            
            scope = f"/subscriptions/{self.config.subscription_id}"
            
            # Convert dates to ISO format strings (matching sample-cost.py)
            # Start of day for from_date, end of day for to_date
            from_datetime = datetime.combine(start_date, datetime.min.time()).replace(tzinfo=timezone.utc)
            to_datetime = datetime.combine(end_date, datetime.max.time()).replace(tzinfo=timezone.utc)
            from_iso = from_datetime.isoformat()
            to_iso = to_datetime.isoformat()
            
            # Create proper QueryAggregation object
            aggregation = QueryAggregation(
                name="PreTaxCost",
                function="Sum"
            )
            
            # Create proper QueryGrouping object
            grouping = QueryGrouping(
                type="Dimension",
                name="ServiceName"
            )
            
            query_definition = QueryDefinition(
                type="Usage",  # Changed from "ActualCost" to "Usage" (matching sample-cost.py)
                timeframe="Custom",
                time_period=QueryTimePeriod(
                    from_property=from_iso,  # Using ISO string format
                    to=to_iso  # Using ISO string format
                ),
                dataset=QueryDataset(
                    granularity="Daily",  # Keeping "Daily" for daily breakdown
                    aggregation={
                        "totalCost": aggregation
                    },
                    grouping=[grouping]
                )
            )
            
            self.logger.info("Querying Azure Cost Management API...")
            self.logger.info(f"Scope: {scope}")
            days_range = (end_date - start_date).days + 1
            self.logger.info(f"Date range: {start_date} to {end_date} ({days_range} days)")
            
            if days_range > 30:
                self.logger.warning(f"Large date range ({days_range} days) - this may take 5-15 minutes. Consider using smaller ranges.")
            
            import time
            import threading
            
            start_time = time.time()
            
            # Show progress indicator
            def show_progress():
                elapsed = 0
                while True:
                    time.sleep(30)  # Update every 30 seconds
                    elapsed = time.time() - start_time
                    if elapsed < 300:  # Show progress for first 5 minutes
                        self.logger.info(f"Still waiting for Azure Cost Management API response... ({elapsed/60:.1f} minutes elapsed)")
                    elif elapsed < 600:  # Every minute after 5 minutes
                        if int(elapsed) % 60 == 0:
                            self.logger.info(f"Still waiting... ({elapsed/60:.1f} minutes elapsed)")
                    else:  # Every 2 minutes after 10 minutes
                        if int(elapsed) % 120 == 0:
                            self.logger.warning(f"Query taking longer than expected... ({elapsed/60:.1f} minutes elapsed)")
            
            progress_thread = threading.Thread(target=show_progress, daemon=True)
            progress_thread.start()
            
            all_records = []
            all_results = []
            
            try:
                # Make initial query
                result = self.cost_client.query.usage(scope=scope, parameters=query_definition)
                all_results.append(result)
                
                # Handle pagination - fetch all pages if next_link exists
                while result and hasattr(result, 'next_link') and result.next_link:
                    self.logger.info(f"Fetching next page of results... (next_link: {result.next_link[:100]}...)")
                    # Note: The SDK might handle next_link automatically, but we'll track it
                    # For now, if next_link exists, we'd need to make another query
                    # The SDK's query.usage might not support direct next_link, so we break
                    # and log that pagination might be needed
                    if result.next_link:
                        self.logger.warning("Pagination detected (next_link present), but SDK may not support direct pagination")
                        self.logger.warning("If data seems incomplete, consider using smaller date ranges")
                    break
                    
            except Exception as api_error:
                error_msg = str(api_error)
                self.logger.error(f"API call failed: {error_msg}", exc_info=True)
                
                # Provide helpful error messages
                if "AADSTS" in error_msg or "authentication" in error_msg.lower():
                    self.logger.error("Authentication error - check your service principal credentials")
                elif "permission" in error_msg.lower() or "authorization" in error_msg.lower():
                    self.logger.error("Permission error - ensure service principal has 'Cost Management Reader' role")
                elif "not found" in error_msg.lower():
                    self.logger.error("Resource not found - check subscription ID and scope")
                elif "throttle" in error_msg.lower() or "rate limit" in error_msg.lower():
                    self.logger.error("Rate limit exceeded - wait and retry with smaller date range")
                
                raise
            finally:
                elapsed_time = time.time() - start_time
                self.logger.info(f"Azure Cost Management API query completed in {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")
            
            # Process all results (including paginated ones)
            for result_idx, result in enumerate(all_results):
                self.logger.info(f"Processing result page {result_idx + 1}/{len(all_results)}")
                
                # Debug: Print the EXACT response - FORCE OUTPUT (only for first result)
                if result_idx == 0:
                    print("\n" + "=" * 80, flush=True)
                    print("EXACT API RESPONSE - FULL DETAILS", flush=True)
                    print("=" * 80 + "\n", flush=True)
                    self.logger.info("=" * 80)
                    self.logger.info("EXACT API RESPONSE - FULL DETAILS")
                    self.logger.info("=" * 80)
                    self.logger.info(f"Result type: {type(result)}")
                    self.logger.info(f"Result class: {result.__class__.__name__}")
                    
                    # Print the full result object - FORCE OUTPUT
                    print("\n--- Full result object (str representation) ---", flush=True)
                    print(str(result), flush=True)
                    
                    # Log important attributes
                    print("\n--- Checking important attributes ---", flush=True)
                    for attr in ['rows', 'columns', 'next_link']:
                        if hasattr(result, attr):
                            value = getattr(result, attr)
                            print(f"{attr}: {value}", flush=True)
                            print(f"{attr} type: {type(value)}", flush=True)
                            if value is not None and hasattr(value, '__len__'):
                                print(f"{attr} length: {len(value)}", flush=True)
                                
                                # If it's rows and empty, provide helpful message
                                if attr == 'rows' and len(value) == 0:
                                    print(f"\n⚠️  ROWS IS EMPTY - No cost data found for this date range", flush=True)
                                    print(f"   This could mean:", flush=True)
                                    print(f"   1. No costs occurred during {start_date} to {end_date}", flush=True)
                                    print(f"   2. Cost data hasn't appeared yet (takes 24-48 hours)", flush=True)
                                    print(f"   3. Try a different date range with known costs", flush=True)
                
                # Process results by date and service
                rows_data = None
                if hasattr(result, 'rows') and result.rows:
                    rows_data = result.rows
                elif hasattr(result, 'properties') and hasattr(result.properties, 'rows') and result.properties.rows:
                    rows_data = result.properties.rows
                elif hasattr(result, 'data') and hasattr(result.data, 'rows') and result.data.rows:
                    rows_data = result.data.rows
                
                if rows_data:
                    self.logger.info(f"Processing {len(rows_data)} rows from API response (page {result_idx + 1})")
                    # Group by date and service
                    costs_by_date_service = {}
                    
                    for idx, row in enumerate(rows_data):
                        try:
                            self.logger.debug(f"Processing row {idx}: {row} (type: {type(row)})")
                            
                            # Handle different row formats
                            # With "Daily" granularity and grouping by "ServiceName", format is:
                            # [cost, date, service_name, currency]
                            if isinstance(row, (list, tuple)):
                                if len(row) >= 3:
                                    # Format: [cost, date, service_name, currency?]
                                    cost = float(row[0])  # Cost is first
                                    date_value = row[1]   # Date (can be int like 20251201 or string)
                                    service_name = str(row[2]) if len(row) > 2 else "Unknown"
                                    
                                    # Convert date from integer format (20251201) to string (2025-12-01)
                                    if isinstance(date_value, int):
                                        date_str = str(date_value)
                                        # Format: YYYYMMDD -> YYYY-MM-DD
                                        if len(date_str) == 8:
                                            date_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                                    else:
                                        date_str = str(date_value)
                                else:
                                    self.logger.warning(f"Row {idx} has insufficient columns: {row}")
                                    continue
                            elif hasattr(row, '__getitem__'):
                                # Try to access as dict-like or object
                                cost = float(row[0]) if len(row) > 0 else 0.0
                                date_value = row[1] if len(row) > 1 else None
                                service_name = str(row[2]) if len(row) > 2 else "Unknown"
                                
                                # Convert date from integer format
                                if isinstance(date_value, int):
                                    date_str = str(date_value)
                                    if len(date_str) == 8:
                                        date_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                                else:
                                    date_str = str(date_value) if date_value else None
                            else:
                                self.logger.warning(f"Row {idx} has unexpected format: {row}")
                                continue
                            
                            if cost > 0:
                                key = (date_str, service_name)
                                if key not in costs_by_date_service:
                                    costs_by_date_service[key] = 0.0
                                costs_by_date_service[key] += cost
                                self.logger.debug(f"Added cost: {service_name} on {date_str}: ${cost}")
                        except Exception as e:
                            self.logger.warning(f"Error processing row {idx}: {row}. Error: {e}")
                            continue
                    
                    self.logger.info(f"Grouped into {len(costs_by_date_service)} unique date/service combinations")
                    
                    # Create CostRecord objects
                    for (date_str, service_name), total_cost in costs_by_date_service.items():
                        try:
                            # Try to parse date - handle different formats
                            if isinstance(date_str, str):
                                # Try ISO format first
                                try:
                                    usage_date = date.fromisoformat(date_str[:10])
                                except ValueError:
                                    # Try other formats
                                    from datetime import datetime
                                    usage_date = datetime.strptime(date_str[:10], '%Y-%m-%d').date()
                            else:
                                usage_date = date_str if isinstance(date_str, date) else start_date
                            
                            record = CostRecord(
                                cloud_provider='azure',
                                service_name=service_name,
                                cost_usd=self._normalize_cost(total_cost),
                                usage_date=usage_date
                            )
                            all_records.append(record)
                        except (ValueError, TypeError) as e:
                            self.logger.warning(f"Failed to parse date or cost: {date_str}, {service_name}, {total_cost}. Error: {e}")
                            continue
                else:
                    if result_idx == 0:  # Only log warning for first page
                        self.logger.warning("No rows data found in API response. Possible reasons:")
                        self.logger.warning("1. No costs for the specified date range")
                        self.logger.warning("2. API response structure is different than expected")
                        self.logger.warning("3. Query returned empty results")
            
            self.logger.info(f"Collected {len(all_records)} cost records via Cost Management API")
            self._log_collection_summary(start_date, end_date, all_records)
            
            return all_records
            
        except Exception as e:
            self.logger.error(f"Failed to collect Azure costs via Cost Management API: {e}", exc_info=True)
            raise
    
    def _collect_costs_via_sponsorship(
        self,
        start_date: date,
        end_date: date
    ) -> List[CostRecord]:
        """Collect costs using Sponsorship portal API (cookie-based)"""
        self.logger.info(f"Collecting Azure Sponsorship costs from {start_date} to {end_date}")
        
        # Calculate total days to process
        total_days = (end_date - start_date).days + 1
        self.logger.info(f"Will process {total_days} day(s) of data")

        try:
            all_records = []
            current_date = start_date
            day_count = 0

            # Azure API returns aggregated data for entire range
            # So we need to call it separately for each day
            while current_date <= end_date:
                day_count += 1
                self.logger.info(f"[{day_count}/{total_days}] Fetching Azure costs for {current_date}...")

                params = {
                    'startDate': current_date.strftime('%Y-%m-%d'),
                    'endDate': current_date.strftime('%Y-%m-%d'),
                    'subscriptionGuid': self.config.subscription_id
                }

                self.logger.debug(f"Request parameters: startDate={params['startDate']}, endDate={params['endDate']}")
                headers = self._get_headers()
                
                self.logger.info(f"[{day_count}/{total_days}] Sending HTTP request to Azure API (timeout: 60s)...")
                response = requests.get(self.api_url, params=params, headers=headers, timeout=60)
                self.logger.info(f"[{day_count}/{total_days}] Received response: HTTP {response.status_code}")

                if response.status_code != 200:
                    self.logger.error(
                        f"[{day_count}/{total_days}] Azure API returned status {response.status_code} for {current_date}"
                    )
                    if response.text:
                        self.logger.error(f"Response body: {response.text[:500]}")
                    current_date += timedelta(days=1)
                    continue

                # Check if response is valid JSON before parsing
                self.logger.info(f"[{day_count}/{total_days}] Parsing response data for {current_date}...")
                try:
                    # Check content type
                    content_type = response.headers.get('Content-Type', '').lower()
                    if 'json' not in content_type:
                        # Check if it's a login page (cookies expired)
                        if 'text/html' in content_type and ('sign in' in response.text.lower() or 'login' in response.text.lower()):
                            self.logger.error(
                                f"[{day_count}/{total_days}] ⚠️  AZURE COOKIES EXPIRED OR INVALID!"
                            )
                            self.logger.error(
                                "The Azure Sponsorship API returned a login page instead of data."
                            )
                            self.logger.error(
                                "Your AZURE_SPONSORSHIP_COOKIES have expired. Please refresh them:"
                            )
                            self.logger.error(
                                "1. Go to https://www.microsoftazuresponsorships.com/Usage"
                            )
                            self.logger.error(
                                "2. Open DevTools (F12) → Network tab"
                            )
                            self.logger.error(
                                "3. Refresh the page and find any API request"
                            )
                            self.logger.error(
                                "4. Copy the 'Cookie' header value"
                            )
                            self.logger.error(
                                "5. Update AZURE_SPONSORSHIP_COOKIES in your .env file"
                            )
                        else:
                            self.logger.warning(
                                f"[{day_count}/{total_days}] Unexpected content type: {content_type}. "
                                f"Response text preview: {response.text[:200]}"
                            )
                    
                    data = response.json()
                except (JSONDecodeError, ValueError) as json_error:
                    content_type = response.headers.get('Content-Type', '').lower()
                    if 'text/html' in content_type:
                        self.logger.error(
                            f"[{day_count}/{total_days}] ⚠️  Received HTML instead of JSON - Cookies may have expired!"
                        )
                        self.logger.error(
                            "Please refresh your AZURE_SPONSORSHIP_COOKIES (see instructions above)"
                        )
                    else:
                        self.logger.error(
                            f"[{day_count}/{total_days}] Failed to parse JSON response for {current_date}: {json_error}"
                        )
                        self.logger.error(f"Response status: {response.status_code}")
                        self.logger.error(f"Response text (first 500 chars): {response.text[:500]}")
                    current_date += timedelta(days=1)
                    continue
                
                # Validate that we got expected data structure
                if not isinstance(data, dict):
                    self.logger.warning(
                        f"[{day_count}/{total_days}] Unexpected response format for {current_date}. "
                        f"Expected dict, got {type(data)}"
                    )
                    current_date += timedelta(days=1)
                    continue
                
                daily_records = self._parse_sponsorship_response(data, current_date)
                self.logger.info(f"[{day_count}/{total_days}] Parsed {len(daily_records)} record(s) for {current_date}")
                all_records.extend(daily_records)

                current_date += timedelta(days=1)
                self.logger.info(f"[{day_count}/{total_days}] Completed processing for {current_date - timedelta(days=1)}")

            self.logger.info(f"Finished processing all {total_days} day(s)")
            self._log_collection_summary(start_date, end_date, all_records)
            self.logger.info(f"Returning {len(all_records)} total cost record(s)")

            return all_records

        except Exception as e:
            self.logger.error(f"Failed to collect Azure Sponsorship costs: {e}", exc_info=True)
            raise

    def _get_headers(self) -> Dict[str, str]:
        """
        Get HTTP headers with cookies for Azure Sponsorship API

        Returns:
            Dictionary of headers
        """
        return {
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Cookie': self.config.sponsorship_cookies,
            'DNT': '1',
            'Referer': 'https://www.microsoftazuresponsorships.com/Usage',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'X-Requested-With': 'XMLHttpRequest'
        }

    def _parse_sponsorship_response(
        self,
        data: Dict,
        usage_date: date
    ) -> List[CostRecord]:
        """
        Parse Azure Sponsorship API response for a single day

        The API returns data in format:
        {
            "TableHeaders": ["Service Name", "Service Resource", "Spend"],
            "TableRows": [
                ["Cognitive Services", "S1 Speech To Text", "$2,354.00"],
                ["Cognitive Services", "gpt-4o-0806-Inp-glbl Tokens", "$820.60"],
                ...
            ]
        }

        Args:
            data: API response JSON
            usage_date: Date to assign costs to

        Returns:
            List of CostRecord objects
        """
        records = []

        table_rows = data.get('TableRows', [])
        if not table_rows:
            self.logger.debug(f"No Azure Sponsorship usage data for {usage_date}")
            return records

        # Group costs by normalized service name
        service_costs = {}

        for row in table_rows:
            try:
                if len(row) < 3:
                    continue

                # Parse row: [Service Name, Service Resource, Spend]
                service_name = row[0]
                service_resource = row[1]
                spend_str = row[2]

                # Normalize service name based on resource type
                normalized_service = self._normalize_service_name(service_name, service_resource)

                # Parse cost: Remove "$" and "," from string like "$2,354.00"
                cost_str = spend_str.replace('$', '').replace(',', '')
                cost_amount = float(cost_str)

                # Skip zero-cost services
                if cost_amount == 0:
                    continue

                # Aggregate costs by normalized service name
                if normalized_service in service_costs:
                    service_costs[normalized_service] += cost_amount
                else:
                    service_costs[normalized_service] = cost_amount

            except (ValueError, IndexError, TypeError) as e:
                self.logger.warning(f"Failed to parse Azure usage row: {row}. Error: {e}")
                continue

        # Create cost records from aggregated data
        for service_name, total_cost in service_costs.items():
            record = CostRecord(
                cloud_provider='azure',
                service_name=service_name,
                cost_usd=self._normalize_cost(total_cost),
                usage_date=usage_date
            )
            records.append(record)

        return records

    def _normalize_service_name(self, service_name: str, service_resource: str) -> str:
        """
        Normalize Azure service names to group related resources

        Args:
            service_name: Service category (e.g., "Cognitive Services")
            service_resource: Specific resource (e.g., "gpt-4o-0806-Inp-glbl Tokens")

        Returns:
            Normalized service name
        """
        resource_lower = service_resource.lower()

        # Azure OpenAI models
        if any(model in resource_lower for model in ['gpt', 'chatgpt', 'davinci', 'embedding', 'ada']):
            return "Azure OpenAI"

        # Speech Services
        if 'speech to text' in resource_lower or 'stt' in resource_lower:
            return "Azure Speech-to-Text"

        if 'text to speech' in resource_lower or 'tts' in resource_lower or 'neural' in resource_lower:
            return "Azure Text-to-Speech"

        # Default: Use service name
        return service_name
