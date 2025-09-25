# app/core/notifications/reminder_system.py
from typing import List, Dict
from datetime import datetime, timedelta
import asyncio
from jinja2 import Template

class AutomatedReminderSystem:
    def __init__(self, email_service, sms_service=None):
        self.email_service = email_service
        self.sms_service = sms_service
        
        # Email templates
        self.templates = {
            'missing_entry': """
            Hi {{user_name}},
            
            We noticed that you have a credit card transaction that needs to be entered in RMS:
            
            Transaction Details:
            - Date: {{transaction_date}}
            - Amount: {{amount}} {{currency}}
            - Merchant: {{merchant}}
            - Last 4 digits: {{card_last_4}}
            
            Please enter this transaction in RMS within the next 24 hours to maintain compliance.
            Days overdue: {{days_overdue}}
            
            [Enter in RMS] {{rms_link}}
            
            Best regards,
            Finance Automation Team
            """,
            
            'missing_invoice': """
            Hi {{user_name}},
            
            Your RMS entry is missing an invoice attachment:
            
            Entry Details:
            - Date: {{entry_date}}
            - Amount: {{amount}} {{currency}}
            - Description: {{description}}
            
            Please upload the invoice/receipt to complete your entry.
            Days pending: {{days_pending}}
            
            [Upload Invoice] {{rms_link}}
            
            Best regards,
            Finance Automation Team
            """,
            
            'weekly_summary': """
            Hi {{user_name}},
            
            Here's your weekly credit card compliance summary:
            
            Performance Metrics:
            - Compliance Rate: {{compliance_rate}}%
            - Pending Entries: {{pending_entries}}
            - Missing Invoices: {{missing_invoices}}
            
            Action Items:
            {{#action_items}}
            - {{description}} ({{urgency}})
            {{/action_items}}
            
            Keep up the good work!
            
            Finance Automation Team
            """
        }
    
    async def send_3_day_reminders(self, compliance_issues: List[ComplianceIssue]) -> Dict:
        """Send automated reminders for 3-day compliance issues"""
        
        sent_count = 0
        failed_count = 0
        results = {
            'emails_sent': 0,
            'sms_sent': 0,
            'failures': [],
            'summary': {}
        }
        
        # Group issues by user
        user_issues = {}
        for issue in compliance_issues:
            user_id = issue.user_id
            if user_id not in user_issues:
                user_issues[user_id] = []
            user_issues[user_id].append(issue)
        
        # Send reminders to each user
        for user_id, issues in user_issues.items():
            try:
                await self._send_user_reminders(user_id, issues)
                sent_count += 1
            except Exception as e:
                failed_count += 1
                results['failures'].append({
                    'user_id': user_id,
                    'error': str(e)
                })
        
        results['emails_sent'] = sent_count
        results['summary'] = {
            'total_users_notified': sent_count,
            'failed_notifications': failed_count,
            'total_issues_addressed': len(compliance_issues)
        }
        
        return results
    
    async def _send_user_reminders(self, user_id: str, issues: List[ComplianceIssue]):
        """Send consolidated reminder to a specific user"""
        
        user_info = await self._get_user_info(user_id)
        
        # Prioritize issues by severity
        high_priority = [i for i in issues if i.severity == 'high']
        medium_priority = [i for i in issues if i.severity == 'medium']
        low_priority = [i for i in issues if i.severity == 'low']
        
        # Send high priority issues immediately
        for issue in high_priority:
            await self._send_individual_reminder(user_info, issue, urgent=True)
        
        # Batch medium/low priority issues
        if medium_priority or low_priority:
            await self._send_batch_reminder(user_info, medium_priority + low_priority)
    
    async def _send_individual_reminder(self, 
                                      user_info: Dict, 
                                      issue: ComplianceIssue, 
                                      urgent: bool = False):
        """Send individual reminder for specific issue"""
        
        template_name = issue.issue_type
        if template_name not in self.templates:
            template_name = 'missing_entry'  # Default template
        
        template = Template(self.templates[template_name])
        
        email_content = template.render(
            user_name=user_info['name'],
            **issue.transaction_details,
            days_overdue=issue.days_overdue,
            rms_link=f"{self.rms_config['base_url']}/creditcard/entry"
        )
        
        subject = f"{'URGENT: ' if urgent else ''}Credit Card Entry Required - {issue.days_overdue} days overdue"
        
        await self.email_service.send_email(
            to=user_info['email'],
            cc=user_info.get('manager_email') if urgent else None,
            subject=subject,
            body=email_content
        )
        
        # Send SMS for urgent issues
        if urgent and self.sms_service and user_info.get('phone'):
            sms_message = f"URGENT: Credit card entry overdue {issue.days_overdue} days. Amount: {issue.transaction_details.get('amount')}. Please update RMS immediately."
            await self.sms_service.send_sms(user_info['phone'], sms_message)
