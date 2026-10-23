module.exports = {
  apps: [
    {
      name: 'whatsapp-server',
      script: './whatsapp.js',
      cwd: './',
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: '500M',
      exp_backoff_restart_delay: 100,
      env: {
        NODE_ENV: 'development'
      },
      env_production: {
        NODE_ENV: 'production'
      },
      error_file: 'logs/whatsapp-error.log',
      out_file: 'logs/whatsapp-out.log',
      log_date_format: 'YYYY-MM-DD HH:mm Z',
      merge_logs: true
    }
  ]
};
