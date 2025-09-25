# app/core/reporting/fx_compliance_reporter.py
import pandas as pd
from typing import Dict, List
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns
from io import BytesIO
import base64

class FXComplianceReporter:
    def __init__(self):
        self.report_templates = {
            'executive_summary': 'Executive FX Compliance Summary',
            'detailed_analysis': 'Detailed FX Transaction Analysis',
            'audit_report': 'Audit-Ready FX Compliance Report'
        }
    
    async def generate_executive_dashboard(self, 
                                         fx_summary: Dict, 
                                         period: str) -> Dict:
        """Generate executive-level FX dashboard"""
        
        # Key metrics for executives
        key_metrics = {
            'compliance_rate': fx_summary.get('compliance_rate', 0),
            'total_fx_transactions': fx_summary.get('total_foreign_transactions', 0),
            'total_markup_cost': fx_summary.get('financial_impact', {}).get('total_markup_cost_inr', 0),
            'average_markup': fx_summary.get('financial_impact', {}).get('average_markup_percentage', 0),
            'high_risk_transactions': fx_summary.get('risk_summary', {}).get('critical', 0) + fx_summary.get('risk_summary', {}).get('high', 0)
        }
        
        # Compliance trend (this would typically pull from historical data)
        compliance_trend = await self._get_compliance_trend()
        
        # Currency exposure analysis
        currency_exposure = fx_summary.get('currency_breakdown', {})
        
        # Risk indicators
        risk_indicators = {
            'critical_transactions': fx_summary.get('risk_summary', {}).get('critical', 0),
            'non_compliant_rate': (fx_summary.get('compliance_summary', {}).get('non_compliant', 0) / 
                                 max(fx_summary.get('analyzed_transactions', 1), 1)) * 100,
            'potential_savings': fx_summary.get('financial_impact', {}).get('potential_savings_at_2pct', 0)
        }
        
        return {
            'period': period,
            'key_metrics': key_metrics,
            'compliance_trend': compliance_trend,
            'currency_exposure': currency_exposure,
            'risk_indicators': risk_indicators,
            'recommendations': self._generate_executive_recommendations(fx_summary)
        }
    
    def _generate_executive_recommendations(self, fx_summary: Dict) -> List[Dict]:
        """Generate actionable recommendations for executives"""
        
        recommendations = []
        
        # Compliance-based recommendations
        compliance_rate = fx_summary.get('compliance_rate', 0)
        if compliance_rate < 80:
            recommendations.append({
                'priority': 'HIGH',
                'category': 'Compliance',
                'title': 'Improve FX Compliance Rate',
                'description': f'Current compliance rate of {compliance_rate:.1f}% is below acceptable threshold',
                'action': 'Review bank partnerships and negotiate better FX rates',
                'impact': 'Risk reduction and cost savings'
            })
        
        # Cost optimization recommendations
        total_markup = fx_summary.get('financial_impact', {}).get('total_markup_cost_inr', 0)
        if total_markup > 50000:  # ₹50K
            potential_savings = fx_summary.get('financial_impact', {}).get('potential_savings_at_2pct', 0)
            recommendations.append({
                'priority': 'MEDIUM',
                'category': 'Cost Optimization',
                'title': 'Optimize FX Costs',
                'description': f'Current markup costs: ₹{total_markup:,.2f}',
                'action': f'Negotiate rates or switch providers. Potential savings: ₹{potential_savings:,.2f}',
                'impact': 'Direct cost reduction'
            })
        
        # Risk management recommendations
        critical_txns = fx_summary.get('risk_summary', {}).get('critical', 0)
        if critical_txns > 0:
            recommendations.append({
                'priority': 'HIGH',
                'category': 'Risk Management',
                'title': 'Address Critical FX Transactions',
                'description': f'{critical_txns} transactions flagged as critical risk',
                'action': 'Review high-markup transactions and implement approval controls',
                'impact': 'Risk mitigation and better controls'
            })
        
        return recommendations
    
    async def generate_audit_report(self, 
                                  fx_analyses: List[FXAnalysis], 
                                  period_start: datetime, 
                                  period_end: datetime) -> Dict:
        """Generate comprehensive audit-ready report"""
        
        # Audit summary
        audit_summary = {
            'report_period': f"{period_start.strftime('%Y-%m-%d')} to {period_end.strftime('%Y-%m-%d')}",
            'total_fx_transactions': len(fx_analyses),
            'audit_methodology': 'Automated FX rate comparison against interbank rates',
            'compliance_framework': 'Internal FX policy with 2.5% markup threshold',
            'data_sources': ['Bank statements', 'RMS entries', 'Fixer.io API', 'RBI reference rates']
        }
        
        # Compliance findings
        compliance_findings = []
        for analysis in fx_analyses:
            if analysis.compliance_status == 'non_compliant':
                finding = {
                    'transaction_id': analysis.transaction_id,
                    'date': analysis.transaction_date,
                    'amount': f"{analysis.foreign_amount} {analysis.foreign_currency}",
                    'markup_percentage': float(analysis.markup_percentage),
                    'markup_amount': float(analysis.markup_amount_inr),
                    'risk_level': analysis.risk_level,
                    'flags': analysis.flags
                }
                compliance_findings.append(finding)
        
        # Statistical analysis
        if fx_analyses:
            markups = [float(a.markup_percentage) for a in fx_analyses]
            statistical_analysis = {
                'mean_markup': sum(markups) / len(markups),
                'median_markup': sorted(markups)[len(markups) // 2],
                'std_deviation': self._calculate_std_dev(markups),
                'min_markup': min(markups),
                'max_markup': max(markups),
                'transactions_above_threshold': sum(1 for m in markups if m > 2.5)
            }
        else:
            statistical_analysis = {}
        
        return {
            'audit_summary': audit_summary,
            'compliance_findings': compliance_findings,
            'statistical_analysis': statistical_analysis,
            'recommendations': self._generate_audit_recommendations(fx_analyses),
            'appendices': {
                'detailed_transactions': fx_analyses,
                'methodology_notes': self._get_methodology_notes()
            }
        }
    
    def _calculate_std_dev(self, values: List[float]) -> float:
        """Calculate standard deviation"""
        if len(values) < 2:
            return 0.0
        
        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / (len(values) - 1)
        return variance ** 0.5
