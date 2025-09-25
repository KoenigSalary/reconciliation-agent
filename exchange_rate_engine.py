# app/core/fx/exchange_rate_engine.py
import aiohttp
import asyncio
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from dataclasses import dataclass

@dataclass
class ExchangeRateData:
    currency_pair: str
    date: datetime
    interbank_rate: Decimal
    central_bank_rate: Optional[Decimal]
    commercial_rate: Optional[Decimal]
    source: str
    confidence_score: float

@dataclass
class FXAnalysis:
    transaction_id: str
    foreign_amount: Decimal
    foreign_currency: str
    inr_charged: Decimal
    transaction_date: datetime
    interbank_rate: Decimal
    actual_rate_used: Decimal
    markup_percentage: Decimal
    markup_amount_inr: Decimal
    benchmark_inr_amount: Decimal
    variance_from_benchmark: Decimal
    risk_level: str  # 'low', 'medium', 'high', 'critical'
    compliance_status: str  # 'compliant', 'review_required', 'non_compliant'
    flags: List[str]

class ExchangeRateEngine:
    def __init__(self):
        self.rate_sources = {
            'fixer': 'https://api.fixer.io/latest',
            'rbi': 'https://www.rbi.org.in/scripts/BS_ViewBulletin.aspx',
            'xe': 'https://api.xe.com/v1/historic_rate',
            'currencylayer': 'http://api.currencylayer.com/historical'
        }
        
        self.markup_thresholds = {
            'low': 1.5,      # 0-1.5%
            'medium': 2.5,   # 1.5-2.5%
            'high': 4.0,     # 2.5-4.0%
            'critical': 4.0  # >4.0%
        }
        
        self.compliance_limits = {
            'amount_variance_inr': 100,  # ±₹100
            'markup_percentage': 2.5     # 2.5%
        }
    
    async def get_historical_rates(self, 
                                 currency: str, 
                                 date: datetime, 
                                 base_currency: str = 'INR') -> List[ExchangeRateData]:
        """Get historical exchange rates from multiple sources"""
        
        currency_pair = f"{currency}/{base_currency}"
        
        # Fetch from multiple sources in parallel
        tasks = [
            self._fetch_fixer_rate(currency, date, base_currency),
            self._fetch_rbi_rate(currency, date),
            self._fetch_xe_rate(currency, date, base_currency),
            self._fetch_currencylayer_rate(currency, date, base_currency)
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter successful results
        valid_rates = []
        for result in results:
            if not isinstance(result, Exception) and result:
                valid_rates.append(result)
        
        return valid_rates
    
    async def _fetch_fixer_rate(self, 
                              currency: str, 
                              date: datetime, 
                              base_currency: str) -> Optional[ExchangeRateData]:
        """Fetch rate from Fixer.io API"""
        try:
            date_str = date.strftime('%Y-%m-%d')
            url = f"https://api.fixer.io/{date_str}"
            
            params = {
                'access_key': self.fixer_api_key,
                'base': currency,
                'symbols': base_currency
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        if data.get('success') and base_currency in data.get('rates', {}):
                            rate = Decimal(str(data['rates'][base_currency]))
                            
                            return ExchangeRateData(
                                currency_pair=f"{currency}/{base_currency}",
                                date=date,
                                interbank_rate=rate,
                                central_bank_rate=None,
                                commercial_rate=None,
                                source='fixer',
                                confidence_score=0.9
                            )
            
            return None
            
        except Exception as e:
            print(f"Error fetching Fixer rate: {e}")
            return None
    
    async def _fetch_rbi_rate(self, 
                            currency: str, 
                            date: datetime) -> Optional[ExchangeRateData]:
        """Fetch rate from RBI (Reserve Bank of India)"""
        try:
            # RBI provides rates for major currencies
            rbi_currencies = ['USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD']
            
            if currency not in rbi_currencies:
                return None
            
            # This would be implemented based on RBI's actual API/data format
            # For now, returning a placeholder structure
            
            # Note: RBI API implementation would go here
            # They provide reference rates that are excellent for compliance
            
            return None  # Placeholder - implement based on actual RBI API
            
        except Exception as e:
            print(f"Error fetching RBI rate: {e}")
            return None
