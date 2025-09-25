# app/core/alerts/intelligent_alert_system.py
from typing import Dict, List, Any
from datetime import datetime
from dataclasses import dataclass
from enum import Enum

class AlertSeverity(Enum):
    INFO = "info"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

@dataclass
class Alert:
    id: str
    severity: AlertSeverity
    category: str
    title: str
    description: str
    affected_entities: List[str]
    recommended_action: str
    created_at: datetime
    data: Dict[str, Any]
    requires_immediate_attention: bool
    estimated_impact: str

class IntelligentAlertSystem:
    def __init__(self, notification_service):
        self.notification_service = notification_service
        
        # Alert thresholds
        self.thresholds = {
            'stripe_match_rate': 95.0,      # %
            'cc_compliance_score': 85.0,     # %
            'fx_markup_critical': 5.0,       # %
            'amount_variance': 10000,        # INR
            'duplicate_transactions': 5,      # count
            'missing_invoices': 10           # count
        }
        
        # Alert routing
        self.alert_routing = {
            'critical': ['finance_head@company.com', 'cfo@company.com'],
            'high': ['finance_head@company.com', 'ap_manager@company.com'],
            'medium': ['ap_manager@company.com'],
            'low': ['finance_team@company.com'],
            'info': ['finance_team@company.com']
        }
    
    async def process_reconciliation_alerts(self, results: Dict) -> List[Alert]:
        """Process all reconciliation results and generate intelligent alerts"""
        
        alerts = []
        
        # Stripe-based alerts
        if results.get('stripe_results'):
            stripe_alerts = await self._generate_stripe_alerts(results['stripe_results'])
            alerts.extend(stripe_alerts)
        
        # Credit Card-based alerts
        if results.get('creditcard_results'):
            cc_alerts = await self._generate_cc_alerts(results['creditcard_results'])
            alerts.extend(cc_alerts)
        
        # FX-based alerts
        if results.get('fx_results'):
            fx_alerts = await self._generate_fx_alerts(results['fx_results'])
            alerts.extend(fx_alerts)
        
        # Cross-system alerts
        cross_system_alerts = await self._generate_cross_system_alerts(results)
        alerts.extend(cross_system_alerts)
        
        # Process and send alerts
        await self._process_and_send_alerts(alerts)
        
        return alerts
    
    async def _generate_stripe_alerts(self, stripe_results: Dict) -> List[Alert]:
        """Generate alerts from Stripe reconciliation results"""
        
        alerts = []
        summary = stripe_results.get('summary', {})
        
        # Low match rate alert
        match_rate = summary.get('transactions', {}).get('match_rate', 100)
        if match_rate < self.thresholds['stripe_match_rate']:
            alerts.append(Alert(
                id=f"stripe_low_match_rate_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                severity=AlertSeverity.HIGH if match_rate < 90 else AlertSeverity.MEDIUM,
                category="Stripe Reconciliation",
                title="Low Stripe Transaction Match Rate",
                description=f"Stripe transaction match rate is {match_rate:.1f}%, below threshold of {self.thresholds['stripe_match_rate']:.1f}%",
                affected_entities=["stripe_integration", "rms_integration"],
                recommended_action="Review unmatched transactions and verify data integration quality",
                created_at=datetime.now(),
                data={
                    'match_rate': match_rate,
                    'threshold': self.thresholds['stripe_match_rate'],
                    'unmatched_count': summary.get('transactions', {}).get('unmatched_stripe', 0)
                },
                requires_immediate_attention=match_rate < 85,
                estimated_impact="Medium - May indicate integration issues"
            ))
        
        # Duplicate transactions alert
        duplicates = summary.get('issues', {}).get('stripe_duplicates', 0)
        if duplicates >= self.thresholds['duplicate_transactions']:
            alerts.append(Alert(
                id=f"stripe_duplicates_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                severity=AlertSeverity.MEDIUM,
                category="Stripe Reconciliation",
                title="Duplicate Stripe Transactions Detected",
                description=f"Found {duplicates} duplicate transaction groups in Stripe data",
                affected_entities=["stripe_payments"],
                recommended_action="Review and resolve duplicate transactions to prevent double billing",
                created_at=datetime.now(),
                data={'duplicate_count': duplicates},
                requires_immediate_attention=duplicates >= 10,
                estimated_impact="Low - Potential revenue impact if not resolved"
            ))
        
        # Refund tracking alerts
        missing_refunds = summary.get('refunds', {}).get('missing_in_rms', 0)
        if missing_refunds > 0:
            alerts.append(Alert(
                id=f"stripe_missing_refunds_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                severity=AlertSeverity.HIGH,
                category="Stripe Reconciliation",
                title="Refunds Not Recorded in RMS",
                description=f"{missing_refunds} Stripe refunds are not recorded in RMS",
                affected_entities=["stripe_refunds", "rms_entries"],
                recommended_action="Update RMS with missing refund entries to maintain accurate records",
                created_at=datetime.now(),
                data={'missing_refund_count': missing_refunds},
                requires_immediate_attention=True,
                estimated_impact="High - Affects financial reporting accuracy"
            ))
        
        return alerts
    
    async def _generate_cc_alerts(self, cc_results: Dict) -> List[Alert]:
        """Generate alerts from credit card compliance results"""
        
        alerts = []
        compliance_data = cc_results.get('compliance_results', {})
        
        # Low compliance score
        compliance_score = compliance_data.get('compliance_score', 100)
        if compliance_score < self.thresholds['cc_compliance_score']:
            severity = AlertSeverity.CRITICAL if compliance_score < 70 else AlertSeverity.HIGH
            
            alerts.append(Alert(
                id=f"cc_low_compliance_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                severity=severity,
                category="Credit Card Compliance",
                title="Low Credit Card Compliance Score",
                description=f"Credit card compliance score is {compliance_score:.1f}%, below threshold",
                affected_entities=["credit_card_users"],
                recommended_action="Send reminders to users with pending entries and review compliance processes",
                created_at=datetime.now(),
                data={
                    'compliance_score': compliance_score,
                    'total_issues': compliance_data.get('total_issues', 0)
                },
                requires_immediate_attention=compliance_score < 70,
                estimated_impact="High - Regulatory and audit risk"
            ))
        
        # High-priority user issues
        high_priority_issues = [
            issue for issue in compliance_data.get('detailed_issues', [])
            if issue.severity == 'high'
        ]
        
        if len(high_priority_issues) >= 5:
            alerts.append(Alert(
                id=f"cc_high_priority_issues_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                severity=AlertSeverity.HIGH,
                category="Credit Card Compliance",
                title="Multiple High-Priority Compliance Issues",
                description=f"{len(high_priority_issues)} high-priority credit card compliance issues detected",
                affected_entities=[issue.user_id for issue in high_priority_issues],
                recommended_action="Address high-priority issues immediately and contact affected users",
                created_at=datetime.now(),
                data={'high_priority_issues': high_priority_issues},
                requires_immediate_attention=True,
                estimated_impact="High - May affect audit readiness"
            ))
        
        return alerts
    
    async def _generate_fx_alerts(self, fx_results: Dict) -> List[Alert]:
        """Generate alerts from FX analysis results"""
        
        alerts = []
        fx_summary = fx_results.get('summary', {})
        
        # Critical FX markup transactions
        critical_fx = fx_summary.get('risk_summary', {}).get('critical', 0)
        if critical_fx > 0:
            alerts.append(Alert(
                id=f"fx_critical_markup_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                severity=AlertSeverity.CRITICAL,
                category="FX Analysis",
                title="Critical FX Markup Transactions",
                description=f"{critical_fx} transactions with critical FX markup levels detected",
                affected_entities=["fx_transactions"],
                recommended_action="Review critical markup transactions and negotiate better rates with bank",
                created_at=datetime.now(),
                data={
                    'critical_count': critical_fx,
                    'total_markup_cost': fx_summary.get('financial_impact', {}).get('total_markup_cost_inr', 0)
                },
                requires_immediate_attention=True,
                estimated_impact="High - Direct financial impact"
            ))
        
        # High total markup cost
        total_markup = fx_summary.get('financial_impact', {}).get('total_markup_cost_inr', 0)
        if total_markup > 50000:  # ₹50K threshold
            alerts.append(Alert(
                id=f"fx_high_cost_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                severity=AlertSeverity.MEDIUM,
                category="FX Analysis",
                title="High FX Markup Costs",
                description=f"Total FX markup costs of ₹{total_markup:,.2f} exceed threshold",
                affected_entities=["fx_costs"],
                recommended_action="Consider renegotiating FX rates or switching to better banking partners",
                created_at=datetime.now(),
                data={
                    'total_markup_cost': total_markup,
                    'potential_savings': fx_summary.get('financial_impact', {}).get('potential_savings_at_2pct', 0)
                },
                requires_immediate_attention=total_markup > 100000,
                estimated_impact=f"Medium - ₹{total_markup:,.2f} in markup costs"
            ))
        
        return alerts
    
    async def _process_and_send_alerts(self, alerts: List[Alert]):
        """Process and send alerts based on severity and routing rules"""
        
        # Group alerts by severity
        alerts_by_severity = {}
        for alert in alerts:
            severity = alert.severity.value
            if severity not in alerts_by_severity:
                alerts_by_severity[severity] = []
            alerts_by_severity[severity].append(alert)
        
        # Send alerts based on severity
        for severity, alert_list in alerts_by_severity.items():
            recipients = self.alert_routing.get(severity, [])
            
            if recipients and alert_list:
                # Create consolidated email for this severity level
                subject = f"Reconciliation Alert - {severity.upper()}: {len(alert_list)} issue(s) detected"
                body = await self._create_alert_email_body(alert_list, severity)
                
                await self.notification_service.send_email(
                    to=recipients,
                    subject=subject,
                    body=body,
                    priority=severity
                )
                
                # Send SMS for critical alerts
                if severity == 'critical':
                    sms_message = f"CRITICAL: {len(alert_list)} critical reconciliation issues detected. Check email for details."
                    for recipient in recipients:
                        phone = await self._get_user_phone(recipient)
                        if phone:
                            await self.notification_service.send_sms(phone, sms_message)
