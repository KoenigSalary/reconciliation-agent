# app/integrations/rms_client.py
import pandas as pd
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import asyncio
import aiofiles

class RMSCreditCardClient:
    def __init__(self, rms_config: dict):
        self.rms_config = rms_config
        self.export_path = rms_config.get('export_path')
        self.api_endpoint = rms_config.get('api_endpoint')
        
    async def get_credit_card_entries(self, 
                                    start_date: datetime, 
                                    end_date: datetime) -> List[Dict]:
        """Fetch credit card entries from RMS export"""
        
        # Method 1: Direct API call (if available)
        if self.api_endpoint:
            return await self._fetch_via_api(start_date, end_date)
        
        # Method 2: Excel export processing
        return await self._process_excel_export(start_date, end_date)
    
    async def get_invoice_later_entries(self) -> List[Dict]:
        """Get all 'Invoice Later' entries that need tracking"""
        
        invoice_later_data = await self._fetch_invoice_later_panel()
        
        processed_entries = []
        for entry in invoice_later_data:
            processed_entry = {
                'entry_id': entry.get('id'),
                'user_id': entry.get('user_id'),
                'user_name': entry.get('user_name'),
                'amount': float(entry.get('amount', 0)),
                'currency': entry.get('currency', 'INR'),
                'transaction_date': self._parse_date(entry.get('transaction_date')),
                'entry_date': self._parse_date(entry.get('entry_date')),
                'description': entry.get('description', ''),
                'vendor': entry.get('vendor', ''),
                'category': entry.get('category', ''),
                'status': entry.get('status', 'pending'),
                'days_pending': (datetime.now() - self._parse_date(entry.get('entry_date'))).days,
                'has_invoice': bool(entry.get('invoice_attachment')),
                'last_reminder_sent': self._parse_date(entry.get('last_reminder')),
                'card_last_4_digits': entry.get('card_digits', ''),
                'approval_status': entry.get('approval_status', 'pending')
            }
            processed_entries.append(processed_entry)
        
        return processed_entries
    
    async def _process_excel_export(self, 
                                   start_date: datetime, 
                                   end_date: datetime) -> List[Dict]:
        """Process RMS Excel export file"""
        
        # Read the latest export file
        df = pd.read_excel(self.export_path)
        
        # Standardize column names (handle variations in export format)
        column_mapping = {
            'Date': 'transaction_date',
            'Transaction Date': 'transaction_date',
            'Amount': 'amount',
            'Amount (INR)': 'amount',
            'Currency': 'currency',
            'Description': 'description',
            'Vendor': 'vendor',
            'User': 'user_name',
            'Employee': 'user_name',
            'Card Number': 'card_number',
            'Card Digits': 'card_last_4',
            'Category': 'category',
            'Invoice': 'invoice_number',
            'Status': 'status'
        }
        
        # Rename columns
        for old_name, new_name in column_mapping.items():
            if old_name in df.columns:
                df = df.rename(columns={old_name: new_name})
        
        # Filter by date range
        df['transaction_date'] = pd.to_datetime(df['transaction_date'])
        df = df[(df['transaction_date'] >= start_date) & (df['transaction_date'] <= end_date)]
        
        # Convert to list of dictionaries
        return df.to_dict('records')
