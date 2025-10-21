#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Multi-URL Nettiauto Monitor
Monitor multiple searches at once!
"""

import json
import os
from enhanced_nettiauto_monitor import EnhancedNettiautoMonitor
import threading
import time
from dotenv import load_dotenv

load_dotenv()

class SearchSpecificMonitor(EnhancedNettiautoMonitor):
    """
    An enhanced monitor that includes the search name in the notification message.
    """
    def __init__(self, config, search_name):
        super().__init__(config)
        self.search_name = search_name

    def _format_listing_message(self, listing):
        message = super()._format_listing_message(listing)
        return f"🔍 <b>Search: {self.search_name}</b>\n\n{message}"

class MultiURLMonitor:
    """Monitor multiple Nettiauto searches simultaneously"""

    def __init__(self, searches_file='searches.json'):
        self.searches = self.load_searches(searches_file)
        self.monitors = []

    def load_searches(self, searches_file):
        """Load searches from a JSON file."""
        try:
            with open(searches_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"Error: {searches_file} not found.")
            return []

    def create_monitor_for_search(self, search: dict):
        """Create a monitor instance for a specific search"""
        config = {
            'monitoring_url': search['url'],
            'scan_interval': search['scan_interval']
        }
        monitor = SearchSpecificMonitor(config, search['name'])
        return monitor

    def run_monitor(self, search: dict):
        """Run a single monitor in a thread"""
        print(f"🚀 Starting monitor for: {search['name']}")
        monitor = self.create_monitor_for_search(search)
        monitor.run()

    def run_all(self):
        """Run all monitors in parallel"""
        print(f"🚗 Starting Multi-URL Monitor with {len(self.searches)} searches...")

        threads = []
        for search in self.searches:
            thread = threading.Thread(
                target=self.run_monitor,
                args=(search,),
                daemon=True,
                name=f"Monitor-{search['name']}"
            )
            thread.start()
            threads.append(thread)
            time.sleep(2)  # Stagger starts

        print(f"✅ All {len(threads)} monitors started!")

        # Keep main thread alive
        try:
            while True:
                # Check if all threads are alive
                alive = sum(1 for t in threads if t.is_alive())
                print(f"📊 Status: {alive}/{len(threads)} monitors running")
                time.sleep(300)  # Status update every 5 minutes
        except KeyboardInterrupt:
            print("\n🛑 Stopping all monitors...")

if __name__ == "__main__":
    multi_monitor = MultiURLMonitor()
    multi_monitor.run_all()
