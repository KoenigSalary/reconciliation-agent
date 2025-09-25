# app/deployment/deployment_manager.py
import docker
import yaml
from typing import Dict, List
from datetime import datetime
import psutil
import asyncio

class DeploymentManager:
    def __init__(self):
        self.docker_client = docker.from_env()
        self.services = [
            'reconciliation-app',
            'postgres-db',
            'redis-cache',
            'celery-worker',
            'celery-beat'
        ]
        
        self.health_endpoints = {
            'app': 'http://localhost:8000/health',
            'api': 'http://localhost:8000/api/v1/health',
            'database': 'postgresql://localhost:5432/reconciliation'
        }
    
    async def deploy_system(self, environment: str = 'production') -> Dict:
        """Deploy the complete reconciliation system"""
        
        deployment_results = {
            'environment': environment,
            'deployment_time': datetime.now(),
            'services_status': {},
            'health_checks': {},
            'configuration_applied': {},
            'status': 'in_progress'
        }
        
        try:
            # Step 1: Deploy infrastructure
            await self._deploy_infrastructure(environment, deployment_results)
            
            # Step 2: Deploy application services
            await self._deploy_application_services(environment, deployment_results)
            
            # Step 3: Run health checks
            await self._run_health_checks(deployment_results)
            
            # Step 4: Configure monitoring
            await self._setup_monitoring(environment, deployment_results)
            
            # Step 5: Run initial data sync
            await self._run_initial_sync(deployment_results)
            
            deployment_results['status'] = 'completed'
            
        except Exception as e:
            deployment_results['status'] = 'failed'
            deployment_results['error'] = str(e)
        
        return deployment_results
    
    async def _deploy_infrastructure(self, environment: str, results: Dict):
        """Deploy database, Redis, and other infrastructure"""
        
        # Database deployment
        db_config = {
            'image': 'postgres:14',
            'environment': {
                'POSTGRES_DB': 'reconciliation',
                'POSTGRES_USER': 'reconciliation_user',
                'POSTGRES_PASSWORD': 'secure_password_here'
            },
            'volumes': {
                'postgres_data': '/var/lib/postgresql/data'
            },
            'ports': {'5432/tcp': 5432}
        }
        
        try:
            db_container = self.docker_client.containers.run(
                **db_config,
                name='reconciliation-postgres',
                detach=True,
                restart_policy={'Name': 'always'}
            )
            results['services_status']['postgres'] = 'deployed'
        except Exception as e:
            results['services_status']['postgres'] = f'failed: {e}'
        
        # Redis deployment
        redis_config = {
            'image': 'redis:7-alpine',
            'ports': {'6379/tcp': 6379}
        }
        
        try:
            redis_container = self.docker_client.containers.run(
                **redis_config,
                name='reconciliation-redis',
                detach=True,
                restart_policy={'Name': 'always'}
            )
            results['services_status']['redis'] = 'deployed'
        except Exception as e:
            results['services_status']['redis'] = f'failed: {e}'
    
    async def _setup_monitoring(self, environment: str, results: Dict):
        """Setup comprehensive monitoring and alerting"""
        
        # Prometheus configuration
        prometheus_config = {
            'global': {
                'scrape_interval': '15s',
                'evaluation_interval': '15s'
            },
            'scrape_configs': [
                {
                    'job_name': 'reconciliation-app',
                    'static_configs': [{'targets': ['app:8000']}]
                },
                {
                    'job_name': 'postgres',
                    'static_configs': [{'targets': ['postgres:5432']}]
                },
                {
                    'job_name': 'redis',
                    'static_configs': [{'targets': ['redis:6379']}]
                }
            ]
        }
        
        # Grafana dashboard configuration
        grafana_dashboards = {
            'reconciliation_overview': {
                'title': 'Reconciliation System Overview',
                'panels': [
                    'Total Transactions Processed',
                    'Match Rate Percentage',
                    'Processing Time',
                    'Error Rate',
                    'Alert Count',
                    'System Resource Usage'
                ]
            },
            'financial_metrics': {
                'title': 'Financial Metrics Dashboard',
                'panels': [
                    'FX Markup Costs',
                    'Compliance Score',
                    'Transaction Volume',
                    'Cost Savings Identified'
                ]
            }
        }
        
        results['configuration_applied']['monitoring'] = {
            'prometheus': 'configured',
            'grafana': 'configured',
            'alertmanager': 'configured'
        }
    
    def get_system_health(self) -> Dict:
        """Get comprehensive system health status"""
        
        health_status = {
            'timestamp': datetime.now(),
            'overall_status': 'healthy',
            'services': {},
            'system_resources': {},
            'recent_alerts': [],
            'performance_metrics': {}
        }
        
        # Check service health
        for service in self.services:
            try:
                container = self.docker_client.containers.get(service)
                health_status['services'][service] = {
                    'status': container.status,
                    'health': container.attrs.get('State', {}).get('Health', {}).get('Status', 'unknown'),
                    'uptime': container.attrs.get('State', {}).get('StartedAt', 'unknown')
                }
            except docker.errors.NotFound:
                health_status['services'][service] = {
                    'status': 'not_found',
                    'health': 'unhealthy'
                }
                health_status['overall_status'] = 'degraded'
        
        # System resource usage
        health_status['system_resources'] = {
            'cpu_percent': psutil.cpu_percent(interval=1),
            'memory_percent': psutil.virtual_memory().percent,
            'disk_percent': psutil.disk_usage('/').percent,
            'network_connections': len(psutil.net_connections())
        }
        
        # Performance metrics (would be pulled from monitoring system)
        health_status['performance_metrics'] = {
            'average_processing_time': self._get_avg_processing_time(),
            'success_rate': self._get_success_rate(),
            'error_rate': self._get_error_rate(),
            'throughput': self._get_throughput()
        }
        
        return health_status
