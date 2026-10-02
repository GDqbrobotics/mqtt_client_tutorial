#!/usr/bin/env python3
"""
Test Script for MQTT Client
Run this to verify the client is working correctly with an MQTT broker.

Usage:
    python test_client.py --config config.yaml
    python test_client.py --config config_test.yaml --verbose
"""

import sys
import time
import json
import yaml

from mqtt_client import MQTTClient, load_config


def main():
    parser_args = sys.argv[1:]
    
    config_path = 'config_test.yaml'
    verbose = False
    
    i = 0
    while i < len(parser_args):
        if parser_args[i] == '--config' and i + 1 < len(parser_args):
            config_path = parser_args[i + 1]
            i += 2
        elif parser_args[i] == '--verbose' or parser_args[i] == '-v':
            verbose = True
            i += 1
        else:
            print(f"Unknown argument: {parser_args[i]}")
            i += 1

    print("=" * 70)
    print("MQTT Client Test")
    print("=" * 70)
    print(f"Configuration: {config_path}")
    print(f"Verbose mode: {verbose}")
    print("-" * 70)

    try:
        # Load configuration
        config = load_config(config_path)
        
        print("Configuration loaded:")
        print(f"  Broker: {config.broker}:{config.port}")
        print(f"  Client ID: {config.client_id}")
        print(f"  Topics configured: {len(config.topics)}")
        for t in config.topics:
            print(f"    - {t.topic} (QoS={t.qos}, {t.direction})")
        print("-" * 70)

        # Create client
        client = MQTTClient(config)

        # Set custom message callback for demonstration
        def on_message_callback(client, userdata, msg):
            """Custom callback for received messages."""
            if verbose:
                print(f"\n[ON_MESSAGE_CALLBACK] Topic: {msg.topic}")
                print(f"[ON_MESSAGE_CALLBACK] Payload: {msg.payload.decode('utf-8', errors='replace')}")
                print(f"[ON_MESSAGE_CALLBACK] QoS: {msg.qos}")
                print(f"[ON_MESSAGE_CALLBACK] Retain: {msg.retain}")

        config.on_message = on_message_callback

        # Start the client
        print("\nConnecting to MQTT broker...")
        if not client.start():
            print("ERROR: Failed to connect to broker!")
            return 1

        print(f"Connected! Broker: {config.broker}:{config.port}")
        print("-" * 70)

        # Publish test messages
        print("\nPublishing test messages...")
        client.publish('test/bidirectional', 'Hello from client!', qos=1)
        time.sleep(0.5)
        client.publish('test/bidirectional', 'Second message', qos=0)
        time.sleep(0.5)

        # Keep running and display stats
        print("-" * 70)
        print("Client is running. Press Ctrl+C to stop.")
        print("  (You can send messages to the broker from another client)")
        print("-" * 70)

        try:
            while True:
                # Update display periodically
                stats = client.get_stats()
                
                if verbose:
                    print(f"\n[STATS] Connection: {'CONNECTED' if stats['connected'] else 'DISCONNECTED'}")
                    print(f"[STATS] Received: {stats['received_count']}, Sent: {stats['sent_count']}")
                    if stats['last_message_time']:
                        print(f"[STATS] Last message: {stats['last_message_time']}s ago")
                
                time.sleep(2)

        except KeyboardInterrupt:
            print("\n\nShutting down...")

        finally:
            # Clean shutdown
            client.stop()
            print("MQTT client stopped successfully.")

    except FileNotFoundError as e:
        print(f"ERROR: Configuration file not found: {e}")
        return 1
    except yaml.YAMLError as e:
        print(f"ERROR: Invalid YAML in configuration file: {e}")
        return 1
    except Exception as e:
        print(f"ERROR: Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
