#!/usr/bin/env python3
"""
Backend API Testing for API Keys Admin Panel
===========================================
Testing the fixes for API Keys admin issues including:
1. Remove Twitter/GitHub from services list
2. Default service empty (not coingecko)
3. Proxy priorities are different  
4. API keys can be bound to proxy via proxy_id
5. CoinGecko proxy rotation system
"""

import requests
import sys
import json
from datetime import datetime
from typing import Dict, List, Any

class APIKeysAdminTester:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.tests_run = 0
        self.tests_passed = 0
        self.issues = []

    def run_test(self, name: str, test_func, expected=True) -> bool:
        """Run a single test and track results"""
        self.tests_run += 1
        print(f"\n🔍 Testing: {name}")
        
        try:
            result = test_func()
            success = (result == expected) if expected is not None else bool(result)
            
            if success:
                self.tests_passed += 1
                print(f"✅ PASSED")
                return True
            else:
                print(f"❌ FAILED - Expected: {expected}, Got: {result}")
                self.issues.append(f"FAILED: {name} - Expected: {expected}, Got: {result}")
                return False
                
        except Exception as e:
            print(f"❌ ERROR - {str(e)}")
            self.issues.append(f"ERROR: {name} - {str(e)}")
            return False

    def test_services_list_excludes_twitter_github(self):
        """Test 1: Twitter and GitHub should be removed from services list"""
        url = f"{self.base_url}/api/admin/api-keys/services"
        response = requests.get(url)
        
        if response.status_code != 200:
            raise Exception(f"Services endpoint failed: {response.status_code}")
            
        data = response.json()
        service_ids = [service['id'] for service in data.get('services', [])]
        
        print(f"   Found services: {service_ids}")
        
        # Should NOT contain twitter or github
        has_twitter = 'twitter' in service_ids
        has_github = 'github' in service_ids
        
        if has_twitter:
            self.issues.append("Twitter still present in services list")
        if has_github:
            self.issues.append("GitHub still present in services list")
            
        return not has_twitter and not has_github

    def test_services_list_content(self):
        """Test 2: Services list should contain expected services (coingecko, coinmarketcap, messari, openai)"""
        url = f"{self.base_url}/api/admin/api-keys/services"
        response = requests.get(url)
        data = response.json()
        service_ids = [service['id'] for service in data.get('services', [])]
        
        expected_services = {'coingecko', 'coinmarketcap', 'messari', 'openai'}
        actual_services = set(service_ids)
        
        print(f"   Expected: {expected_services}")
        print(f"   Actual: {actual_services}")
        
        return expected_services.issubset(actual_services)

    def test_add_api_key_with_proxy_binding(self):
        """Test 3: API keys can be bound to specific proxy via proxy_id parameter"""
        url = f"{self.base_url}/api/admin/api-keys"
        test_data = {
            "service": "coingecko",
            "api_key": "test_key_12345678",
            "name": "Test CoinGecko Key",
            "is_pro": False,
            "proxy_id": "proxy_test_123"  # This should be accepted
        }
        
        response = requests.post(url, json=test_data)
        
        if response.status_code == 200:
            result = response.json()
            print(f"   API Key added successfully with ID: {result.get('id')}")
            
            # Clean up - try to delete the test key
            if 'id' in result:
                delete_url = f"{self.base_url}/api/admin/api-keys/{result['id']}"
                requests.delete(delete_url)
            
            return True
        else:
            print(f"   Failed to add API key: {response.status_code} - {response.text}")
            return False

    def test_add_api_key_requires_service_selection(self):
        """Test 4: Adding API key should require service selection (not allow empty service)"""
        url = f"{self.base_url}/api/admin/api-keys"
        test_data = {
            "service": "",  # Empty service should fail
            "api_key": "test_key_12345678",
            "name": "Test Key"
        }
        
        response = requests.post(url, json=test_data)
        
        # Should fail with 400 error for empty/invalid service
        success = response.status_code == 400
        if not success:
            print(f"   Expected 400 error for empty service, got: {response.status_code}")
        return success

    def test_api_keys_list_endpoint(self):
        """Test 5: List API keys endpoint works"""
        url = f"{self.base_url}/api/admin/api-keys"
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            print(f"   Found {data.get('total', 0)} API keys")
            return True
        else:
            print(f"   Failed to fetch API keys: {response.status_code}")
            return False

    def test_proxy_endpoints_exist(self):
        """Test 6: Check if proxy management endpoints exist (for proxy priorities)"""
        # Try multiple possible proxy endpoints
        proxy_endpoints = [
            f"{self.base_url}/api/admin/proxies",
            f"{self.base_url}/api/proxies", 
            f"{self.base_url}/api/system/proxies"
        ]
        
        for endpoint in proxy_endpoints:
            response = requests.get(endpoint)
            if response.status_code == 200:
                print(f"   Proxy endpoint found: {endpoint}")
                return True
            elif response.status_code != 404:
                print(f"   Proxy endpoint {endpoint} returned: {response.status_code}")
        
        print(f"   No proxy endpoints found (this might be expected)")
        return False  # This might be expected if proxies aren't implemented yet

    def test_service_validation(self):
        """Test 7: Service validation works correctly"""
        url = f"{self.base_url}/api/admin/api-keys"
        
        # Test with invalid service
        test_data = {
            "service": "invalid_service",
            "api_key": "test_key_12345678",
            "name": "Test Key"
        }
        
        response = requests.post(url, json=test_data)
        
        # Should fail with 400 error for invalid service
        success = response.status_code == 400
        if success:
            print(f"   Correctly rejected invalid service")
        else:
            print(f"   Should have rejected invalid service, got: {response.status_code}")
        return success

    def test_coingecko_proxy_rotation_setup(self):
        """Test 8: Check if CoinGecko can be configured for proxy rotation"""
        # Test adding multiple CoinGecko keys with different proxy bindings
        url = f"{self.base_url}/api/admin/api-keys"
        
        test_keys = [
            {
                "service": "coingecko",
                "api_key": "cg_test_key_1",
                "name": "CoinGecko Proxy 1",
                "proxy_id": "proxy_1"
            },
            {
                "service": "coingecko", 
                "api_key": "cg_test_key_2",
                "name": "CoinGecko Proxy 2",
                "proxy_id": "proxy_2"
            }
        ]
        
        added_ids = []
        success_count = 0
        
        for key_data in test_keys:
            response = requests.post(url, json=key_data)
            if response.status_code == 200:
                result = response.json()
                added_ids.append(result.get('id'))
                success_count += 1
                print(f"   Added CoinGecko key with proxy: {key_data['proxy_id']}")
        
        # Clean up
        for key_id in added_ids:
            if key_id:
                delete_url = f"{self.base_url}/api/admin/api-keys/{key_id}"
                requests.delete(delete_url)
        
        return success_count == 2

    def print_results(self):
        """Print final test results"""
        print(f"\n{'='*60}")
        print(f"API KEYS ADMIN TEST RESULTS")
        print(f"{'='*60}")
        print(f"Tests Run: {self.tests_run}")
        print(f"Tests Passed: {self.tests_passed}")
        print(f"Tests Failed: {self.tests_run - self.tests_passed}")
        print(f"Success Rate: {(self.tests_passed/self.tests_run*100):.1f}%")
        
        if self.issues:
            print(f"\n🚨 ISSUES FOUND:")
            for issue in self.issues:
                print(f"   • {issue}")
        else:
            print(f"\n✅ ALL TESTS PASSED!")
        
        return self.tests_passed == self.tests_run


def main():
    base_url = "https://db-bootstrap-deploy-1.preview.emergentagent.com"
    tester = APIKeysAdminTester(base_url)

    print(f"🚀 Starting API Keys Admin Panel Testing...")
    print(f"Backend URL: {base_url}")

    # Run all tests
    tester.run_test(
        "Services list excludes Twitter and GitHub",
        tester.test_services_list_excludes_twitter_github
    )
    
    tester.run_test(
        "Services list contains expected services",
        tester.test_services_list_content
    )
    
    tester.run_test(
        "API key can be bound to proxy via proxy_id",
        tester.test_add_api_key_with_proxy_binding
    )
    
    tester.run_test(
        "Adding API key requires valid service selection",
        tester.test_add_api_key_requires_service_selection
    )
    
    tester.run_test(
        "API keys list endpoint works",
        tester.test_api_keys_list_endpoint
    )
    
    # This test might fail if proxies aren't implemented yet - that's okay
    tester.run_test(
        "Proxy management endpoints exist",
        tester.test_proxy_endpoints_exist,
        expected=None  # Don't count as failure if not implemented
    )
    
    tester.run_test(
        "Service validation works correctly", 
        tester.test_service_validation
    )
    
    tester.run_test(
        "CoinGecko proxy rotation can be configured",
        tester.test_coingecko_proxy_rotation_setup
    )

    # Print final results
    success = tester.print_results()
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())