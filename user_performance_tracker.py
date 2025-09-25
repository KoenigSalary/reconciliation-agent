# app/core/tracking/user_performance_tracker.py
from typing import List, Dict
from datetime import datetime, timedelta
from dataclasses import dataclass

@dataclass
class UserPerformanceMetrics:
    user_id: str
    user_name: str
    total_transactions: int
    timely_entries: int
    late_entries: int
    missing_entries: int
    average_entry_delay_hours: float
    compliance_rate: float
    invoice_attachment_rate: float
    total_amount_handled: float
    risk_score: float
    trend: str  # 'improving', 'declining', 'stable'

class UserPerformanceTracker:
    def __init__(self):
        self.performance_window_days = 90
        
    async def generate_user_performance_report(self, 
                                             users: List[str], 
                                             period_days: int = 30) -> List[UserPerformanceMetrics]:
        """Generate comprehensive user performance metrics"""
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=period_days)
        
        user_metrics = []
        
        for user_id in users:
            metrics = await self._calculate_user_metrics(user_id, start_date, end_date)
            user_metrics.append(metrics)
        
        # Sort by compliance rate (ascending) to highlight problem areas
        user_metrics.sort(key=lambda x: x.compliance_rate)
        
        return user_metrics
    
    async def _calculate_user_metrics(self, 
                                    user_id: str, 
                                    start_date: datetime, 
                                    end_date: datetime) -> UserPerformanceMetrics:
        """Calculate detailed metrics for a specific user"""
        
        # Get user's transactions from RMS and bank statements
        user_rms_entries = await self._get_user_rms_entries(user_id, start_date, end_date)
        user_bank_txns = await self._get_user_bank_transactions(user_id, start_date, end_date)
        
        # Calculate metrics
        total_bank_transactions = len(user_bank_txns)
        timely_entries = 0
        late_entries = 0
        missing_entries = 0
        total_delay_hours = 0
        entries_with_invoices = 0
        
        for bank_txn in user_bank_txns:
            matching_rms = self._find_matching_rms_entry(bank_txn, user_rms_entries)
            
            if matching_rms:
                # Calculate entry delay
                txn_date = bank_txn['transaction_date']
                entry_date = matching_rms.get('entry_date', txn_date)
                delay_hours = (entry_date - txn_date).total_seconds() / 3600
                
                if delay_hours <= 72:  # Within 3 days
                    timely_entries += 1
                else:
                    late_entries += 1
                
                total_delay_hours += delay_hours
                
                # Check invoice attachment
                if matching_rms.get('has_invoice', False):
                    entries_with_invoices += 1
            else:
                missing_entries += 1
        
        # Calculate rates and scores
        compliance_rate = (timely_entries / total_bank_transactions * 100) if total_bank_transactions > 0 else 0
        invoice_attachment_rate = (entries_with_invoices / (timely_entries + late_entries) * 100) if (timely_entries + late_entries) > 0 else 0
        average_delay = total_delay_hours / (timely_entries + late_entries) if (timely_entries + late_entries) > 0 else 0
        
        # Calculate risk score (higher is riskier)
        risk_score = self._calculate_risk_score(missing_entries, late_entries, total_bank_transactions, invoice_attachment_rate)
        
        # Determine trend
        trend = await self._calculate_trend(user_id, end_date)
        
        return UserPerformanceMetrics(
            user_id=user_id,
            user_name=await self._get_user_name(user_id),
            total_transactions=total_bank_transactions,
            timely_entries=timely_entries,
            late_entries=late_entries,
            missing_entries=missing_entries,
            average_entry_delay_hours=average_delay,
            compliance_rate=compliance_rate,
            invoice_attachment_rate=invoice_attachment_rate,
            total_amount_handled=sum(txn.get('amount', 0) for txn in user_bank_txns),
            risk_score=risk_score,
            trend=trend
        )
    
    def _calculate_risk_score(self, 
                            missing: int, 
                            late: int, 
                            total: int, 
                            invoice_rate: float) -> float:
        """Calculate user risk score (0-100, higher is riskier)"""
        
        if total == 0:
            return 0
        
        missing_weight = 40
        late_weight = 30
        invoice_weight = 30
        
        missing_score = (missing / total) * missing_weight
        late_score = (late / total) * late_weight
        invoice_score = (100 - invoice_rate) / 100 * invoice_weight
        
        return min(missing_score + late_score + invoice_score, 100)
