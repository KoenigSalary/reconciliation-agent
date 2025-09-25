# app/core/reconciliation/fx_reconciler.py
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import pandas as pd

class FXReconciler:
    def __init__(self, fx_detector, markup_analyzer, rate_engine):
        self.fx_detector = fx_detector
        self.markup_analyzer = markup_analyzer
        self.rate_engine = rate_engine
    
    async def analyze_transactions_batch(self, 
                                       transactions: List[Dict], 
                                       generate_report: bool = True) -> Dict:
        """Analyze a batch of transactions for FX compliance"""
        
        fx_analyses = []
        domestic_count = 0
        foreign_count = 0
        
        # Process each transaction
        for transaction in transactions:
            # Detect if transaction is foreign
            fx_details = self.fx_detector.detect_fx_transaction(
                transaction.get('description', ''),
                transaction.get('amount', 0),
                transaction.get('merchant', '')
            )
            
            if fx_details.is_foreign:
                foreign_count += 1
                # Perform detailed FX analysis
                analysis = await self.markup_analyzer.analyze_fx_transaction(transaction, fx_details)
                fx_analyses.append(analysis)
            else:
                domestic_count += 1
        
        # Generate summary statistics
        summary = self._generate_fx_summary(fx_analyses, domestic_count, foreign_count)
        
        # Generate detailed report if requested
        report_data = None
        if generate_report:
            report_data = await self._generate_detailed_report(fx_analyses, summary)
        
        return {
            'summary': summary,
            'fx_analyses': fx_analyses,
            'report_data': report_data,
            'processing_stats': {
                'total_transactions': len(transactions),
                'domestic_transactions': domestic_count,
                'foreign_transactions': foreign_count,
                'analysis_completion_rate': len(fx_analyses) / max(foreign_count, 1) * 100
            }
        }
    
    def _generate_fx_summary(self, 
                           analyses: List[FXAnalysis], 
                           domestic_count: int, 
                           foreign_count: int) -> Dict:
        """Generate comprehensive FX summary statistics"""
        
        if not analyses:
            return {
                'total_foreign_transactions': 0,
                'total_domestic_transactions': domestic_count,
                'compliance_summary': {'compliant': 0, 'review_required': 0, 'non_compliant': 0},
                'risk_summary': {'low': 0, 'medium': 0, 'high': 0, 'critical': 0},
                'average_markup': 0,
                'total_markup_cost': 0,
                'currency_breakdown': {}
            }
        
        # Compliance breakdown
        compliance_counts = {'compliant': 0, 'review_required': 0, 'non_compliant': 0}
        for analysis in analyses:
            compliance_counts[analysis.compliance_status] += 1
        
        # Risk breakdown
        risk_counts = {'low': 0, 'medium': 0, 'high': 0, 'critical': 0}
        for analysis in analyses:
            risk_counts[analysis.risk_level] += 1
        
        # Financial impact
        total_markup = sum(float(a.markup_amount_inr) for a in analyses)
        average_markup = sum(float(a.markup_percentage) for a in analyses) / len(analyses)
        
        # Currency breakdown
        currency_stats = {}
        for analysis in analyses:
            currency = analysis.foreign_currency
            if currency not in currency_stats:
                currency_stats[currency] = {
                    'transaction_count': 0,
                    'total_foreign_amount': 0,
                    'total_inr_amount': 0,
                    'total_markup': 0,
                    'average_markup_percentage': 0
                }
            
            currency_stats[currency]['transaction_count'] += 1
            currency_stats[currency]['total_foreign_amount'] += float(analysis.foreign_amount)
            currency_stats[currency]['total_inr_amount'] += float(analysis.inr_charged)
            currency_stats[currency]['total_markup'] += float(analysis.markup_amount_inr)
        
        # Calculate average markup percentage per currency
        for currency, stats in currency_stats.items():
            currency_analyses = [a for a in analyses if a.foreign_currency == currency]
            stats['average_markup_percentage'] = sum(float(a.markup_percentage) for a in currency_analyses) / len(currency_analyses)
        
        return {
            'total_foreign_transactions': foreign_count,
            'total_domestic_transactions': domestic_count,
            'analyzed_transactions': len(analyses),
            'compliance_summary': compliance_counts,
            'risk_summary': risk_counts,
            'financial_impact': {
                'total_markup_cost_inr': round(total_markup, 2),
                'average_markup_percentage': round(average_markup, 2),
                'highest_single_markup': max(float(a.markup_amount_inr) for a in analyses) if analyses else 0,
                'potential_savings_at_2pct': round(total_markup - sum(float(a.foreign_amount) * float(a.interbank_rate) * 0.02 for a in analyses), 2)
            },
            'currency_breakdown': currency_stats,
            'flagged_transactions': sum(1 for a in analyses if a.flags),
            'compliance_rate': compliance_counts['compliant'] / len(analyses) * 100 if analyses else 0
        }
    
    async def _generate_detailed_report(self, 
                                      analyses: List[FXAnalysis], 
                                      summary: Dict) -> Dict:
        """Generate detailed Excel-ready report data"""
        
        # Main analysis data
        report_rows = []
        for analysis in analyses:
            row = {
                'Transaction ID': analysis.transaction_id,
                'Date': analysis.transaction_date.strftime('%Y-%m-%d') if analysis.transaction_date else '',
                'Foreign Amount': f"{analysis.foreign_amount} {analysis.foreign_currency}",
                'INR Charged': f"₹{analysis.inr_charged}",
                'Interbank Rate': f"{analysis.interbank_rate}",
                'Bank Rate Used': f"{analysis.actual_rate_used}",
                'Markup %': f"{analysis.markup_percentage}%",
                'Markup Amount (INR)': f"₹{analysis.markup_amount_inr}",
                'Expected INR': f"₹{analysis.benchmark_inr_amount}",
                'Variance': f"₹{analysis.variance_from_benchmark}",
                'Risk Level': analysis.risk_level.upper(),
                'Compliance': analysis.compliance_status.replace('_', ' ').title(),
                'Flags': ', '.join(analysis.flags) if analysis.flags else 'None'
            }
            report_rows.append(row)
        
        # Summary statistics for report header
        report_summary = {
            'Report Generated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'Total Transactions Analyzed': len(analyses),
            'Compliance Rate': f"{summary['compliance_rate']:.1f}%",
            'Total Markup Cost': f"₹{summary['financial_impact']['total_markup_cost_inr']}",
            'Average Markup': f"{summary['financial_impact']['average_markup_percentage']:.2f}%"
        }
        
        return {
            'transaction_details': report_rows,
            'summary_stats': report_summary,
            'compliance_breakdown': summary['compliance_summary'],
            'risk_breakdown': summary['risk_summary'],
            'currency_breakdown': summary['currency_breakdown']
        }
