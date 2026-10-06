#!/usr/bin/env python3
"""
Vanta XRF API Client

A simple Python script to connect to a Vanta XRF instrument and trigger acquisitions.
Supports both USB (OTG) and LAN connections.

Usage:
    python vanta_api_client.py usb
    python vanta_api_client.py lan
    python vanta_api_client.py --ip 192.168.7.2
"""

import sys
import json
import time
import signal
import argparse
from websocket import create_connection, WebSocket, WebSocketException


class VantaClient:
    """Client for communicating with Vanta XRF instrument via API."""
    
    DEFAULT_USB_PORT = 7860
    DEFAULT_LAN_PORT = 7860
    
    def __init__(self, connection_type="usb", host=None):
        """
        Initialize the Vanta client.
        
        Args:
            connection_type: "usb" or "lan"
            host: IP address for LAN connection (required for lan type)
        """
        self.connection_type = connection_type
        self.host = host
        self.ws = None
        self.connected = False
        self.logged_in = False
        
        # Default host for USB connection (usually discovered automatically)
        if connection_type == "usb" and not host:
            self.host = "192.168.7.2"  # Default USB address
        elif connection_type == "lan" and not host:
            print("Error: Host IP address required for LAN connection")
            print("Usage: python vanta_api_client.py lan --ip 192.168.7.2")
            sys.exit(1)
    
    def discover_device(self):
        """
        Attempt to discover the Vanta device on the network.
        Returns the device IP address if found, None otherwise.
        """
        print("\n[*] Scanning for Vanta device...")
        
        # For USB connections, the device is typically at a fixed address
        # through the OTG port
        if self.connection_type == "usb":
            print("[*] USB connection: using default address 192.168.7.2")
            return self.host
        
        # For LAN connections, try to discover via ARP or ping
        if self.connection_type == "lan":
            import subprocess
            try:
                # Common Vanta IP ranges to scan
                ranges = ["192.168.7.x", "192.168.1.x", "10.0.0.x"]
                
                for range_base in ranges:
                    for i in range(1, 256):
                        ip = f"{range_base}.{i}"
                        # Simple ping check
                        try:
                            result = subprocess.run(
                                ["ping", "-c", "1", "-W", "1", ip],
                                capture_output=True,
                                text=True,
                                timeout=2
                            )
                            if result.returncode == 0:
                                # Check if it responds to our connection attempt
                                try:
                                    test_ws = create_connection(
                                        f"ws://{ip}:{self.DEFAULT_LAN_PORT}/"
                                    )
                                    test_ws.close()
                                    print(f"[*] Found Vanta device at {ip}")
                                    return ip
                                except:
                                    continue
                        except:
                            continue
            except Exception as e:
                print(f"[!] Network scan failed: {e}")
            
            print("[!] Device not found on common networks")
            return None
        
        return self.host
    
    def connect(self):
        """
        Establish WebSocket connection to the Vanta instrument.
        """
        print("\n[*] Connecting to Vanta instrument...")
        
        # For USB connections, we can use the OTG port directly
        if self.connection_type == "usb":
            print("[*] Using OTG/USB connection")
            print(f"[*] Connecting to {self.host}:{self.DEFAULT_USB_PORT}")
        else:
            print(f"[*] Connecting to {self.host}:{self.DEFAULT_LAN_PORT}")
        
        try:
            # Use websocket-client library (pip install websocket-client)
            uri = f"ws://{self.host}:{self.DEFAULT_LAN_PORT}/"
            self.ws = create_connection(uri)
            
            self.connected = True
            print("[+] Connection established")
            return True
            
        except WebSocketException as e:
            print(f"[!] WebSocket error: {e}")
            return False
        except Exception as e:
            print(f"[!] Connection failed: {e}")
            return False
    
    def send_command(self, command):
        """
        Send a JSON command to the instrument.
        
        Args:
            command: Dictionary containing the command structure
            
        Returns:
            Response dictionary or None if failed
        """
        if not self.ws or not self.connected:
            print("[!] Not connected")
            return None
        
        try:
            # Send as binary (as specified in documentation)
            self.ws.send_binary(json.dumps(command).encode('utf-8'))
            print(f"[*] Sent: {json.dumps(command)}")
            
            # Wait for response
            return self._receive_response()
            
        except WebSocketException as e:
            print(f"[!] Send error: {e}")
            return None
        except Exception as e:
            print(f"[!] Command error: {e}")
            return None
    
    def _receive_response(self, timeout=5):
        """
        Receive and parse the response from the instrument.
        
        Args:
            timeout: Maximum time to wait for response
            
        Returns:
            Parsed JSON response or None
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                message = self.ws.recv()
                return json.loads(message)
            except WebSocketException:
                # Connection closed or error
                return None
            except json.JSONDecodeError:
                # Not a JSON response, skip
                pass
        
        print("[!] Response timeout")
        return None
    
    def login(self, user_id="Administrator", password="0000"):
        """
        Authenticate with the instrument.
        
        Args:
            user_id: User ID to log in as
            password: User password
            
        Returns:
            True if login successful, False otherwise
        """
        print("\n[*] Logging in as 'Administrator' with password '0000'...")
        
        command = {
            "commandId": 301,
            "id": 1,
            "params": {
                "userId": user_id,
                "password": password
            }
        }
        
        response = self.send_command(command)
        
        if not response:
            print("[!] Login failed - no response")
            return False
        
        if "error" in response:
            print(f"[!] Login failed: {response.get('error', {}).get('errorString', 'Unknown error')}")
            return False
        
        print(f"[+] Logged in as {response.get('params', {}).get('userId', user_id)}")
        self.logged_in = True
        return True
    
    def clear_faults(self):
        """
        Clear any existing faults on the instrument.
        """
        print("\n[*] Clearing faults...")
        
        command = {
            "commandId": 254,
            "id": 1,
            "params": {}
        }
        
        response = self.send_command(command)
        if response:
            print("[+] Faults cleared")
        else:
            print("[!] Failed to clear faults")
    
    def start_test(self):
        """
        Start an XRF test on the instrument.
        """
        print("\n[*] Starting XRF test...")
        
        command = {
            "commandId": 601,
            "id": 1,
            "params": {}
        }
        
        response = self.send_command(command)
        
        if response:
            print("[+] Test started")
            return True
        else:
            print("[!] Failed to start test")
            return False
    
    def stop_test(self):
        """
        Stop the current XRF test.
        """
        print("\n[*] Stopping XRF test...")
        
        command = {
            "commandId": 602,
            "id": 1,
            "params": {}
        }
        
        response = self.send_command(command)
        
        if response:
            print("[+] Test stopped")
        else:
            print("[!] Failed to stop test")
    
    def heartbeat(self):
        """
        Send a Pet Watchdog command to keep the connection alive.
        Must be sent at least once every 4 seconds during a test.
        """
        command = {
            "commandId": 244,
            "id": 1,
            "params": {}
        }
        
        self.send_command(command)
    
    def wait_for_result(self):
        """
        Wait for test completion and result.
        """
        print("\n[*] Waiting for test completion...")
        
        while True:
            try:
                # Use a threaded reader to listen for notifications
                self._listen_for_notification()
            except KeyboardInterrupt:
                print("\n[*] Interrupted by user")
                break
    
    def _listen_for_notification(self):
        """
        Listen for notifications from the instrument in a non-blocking way.
        """
        if not self.ws or not self.connected:
            return
        
        try:
            message = self.ws.recv()
            data = json.loads(message)
            
            command_id = data.get("commandId")
            notification_id = data.get("id")
            
            # Handle different notification types
            if command_id == 403:  # Notification
                if notification_id == 200:  # Test Started
                    print("[+] Test started")
                elif notification_id == 201:  # Test Stopped
                    result_data = data.get("params", {}).get("stopReason", "Unknown")
                    print(f"[+] Test stopped: {result_data}")
                elif notification_id == 206:  # Result Received
                    result = data.get("params", {}).get("result")
                    if result:
                        print("\n[+] RESULT RECEIVED:")
                        print(f"    Method: {result.get('analysis', {}).get('methodDispName', 'Unknown')}")
                        print(f"    Test ID: {result.get('analysis', {}).get('testId', 'N/A')}")
                        print(f"    Date/Time: {result.get('analysis', {}).get('testDateTime', 'N/A')}")
                        
                        # Print chemistry summary
                        chemistry = result.get("chemistry", [])
                        if chemistry:
                            print("\n    Chemistry (top 5 elements):")
                            for elem in chemistry[:5]:
                                name = elem.get("elementName", "Unknown")
                                conc = elem.get("concentration", 0)
                                if conc > 0:
                                    print(f"      {name}: {conc:.2f}%")
                        
                        print("\n[+] Acquisition complete!")
                elif notification_id == 207:  # Exposure Status
                    params = data.get("params", {})
                    print(f"[*] Exposure {params.get('exposureNum', '?')} of {params.get('maxExposures', '?')}, "
                          f"elapsed: {params.get('elapsedTime', '?')}s")
                elif notification_id == 100:  # System Status
                    status = data.get("params", {}).get("systemStatus", "Unknown")
                    print(f"[*] System status: {status}")
            else:
                # Handle command responses
                command_id = data.get("commandId")
                if command_id == 301:  # Login response
                    print(f"[+] Login confirmed: {data.get('params', {}).get('userId', 'Unknown')}")
        except WebSocketException:
            print("[!] Connection lost")
            self.connected = False


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Vanta XRF API Client - Connect and trigger acquisitions"
    )
    parser.add_argument(
        "connection_type",
        nargs="?",
        choices=["usb", "lan"],
        default="usb",
        help="Connection type: usb (OTG) or lan (network)"
    )
    parser.add_argument(
        "--ip",
        default=None,
        help="IP address of the Vanta instrument (required for lan, optional for usb)"
    )
    
    args = parser.parse_args()
    
    # Set up signal handlers for graceful shutdown
    def signal_handler(sig, frame):
        print("\n[*] Shutting down...")
        if client.ws:
            try:
                client.ws.close()
            except:
                pass
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Initialize client
    client = VantaClient(
        connection_type=args.connection_type,
        host=args.ip
    )
    
    # Discover device
    host = client.discover_device()
    
    if not host:
        print("\n[!] Could not find Vanta device")
        print("\nTroubleshooting:")
        print("  - For USB: Ensure the OTG cable is connected properly")
        print("  - For LAN: Verify the IP address and network connectivity")
        sys.exit(1)
    
    # Connect to instrument
    if not client.connect():
        sys.exit(1)
    
    # Login
    if not client.login():
        print("\n[!] Login failed. Check credentials.")
        sys.exit(1)
    
    # Clear any existing faults
    client.clear_faults()
    
    # Main interaction loop
    print("\n" + "="*60)
    print("VANTA XRF API CLIENT")
    print("="*60)
    print("\nConnection type: {}".format(client.connection_type))
    print("Instrument: {}".format(host))
    print("\nCommands:")
    print("  'start'  - Start a new XRF test")
    print("  'stop'   - Stop the current test")
    print("  'reset'  - Reset and start a new test")
    print("  'exit'   - Close connection and exit")
    print("\nEnter command or press Enter to continue monitoring...")
    print("-"*60)
    
    try:
        while True:
            try:
                user_input = input("\n> ").strip().lower()
            except (EOFError, KeyboardInterrupt):
                print("\n[*] Exiting...")
                break
            
            if user_input == "start":
                client.start_test()
                client.wait_for_result()
            
            elif user_input == "stop":
                client.stop_test()
            
            elif user_input == "reset":
                client.stop_test()
                time.sleep(1)
                client.start_test()
                client.wait_for_result()
            
            elif user_input == "exit":
                print("\n[*] Closing connection...")
                break
            
            elif user_input:
                print("[!] Unknown command: '{}'. Type 'help' for available commands.".format(user_input))
            
            # Send heartbeat to keep connection alive during test
            if client.ws and client.connected:
                client.heartbeat()
    
    except Exception as e:
        print("\n[!] Unexpected error: {}".format(e))
    
    finally:
        # Cleanup
        if client.ws:
            try:
                client.ws.close()
            except:
                pass
        print("\n[*] Goodbye!")


if __name__ == "__main__":
    main()
