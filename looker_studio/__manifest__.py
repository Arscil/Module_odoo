{
    'name': 'Looker Studio Reports',
    'version': '2.0.0',
    'summary': 'Looker Studio reports lookalike inside Odoo',
    'description': 'Module to make reports in Odoo.',
    'category': 'Reporting',
    'author': 'Your Name',
    'depends': ['base', 'web', 'website'],
    'data': [
        'security/ir.model.access.csv',
        'views/report_views.xml',
        'views/order_report_views.xml',
        'views/website_templates.xml',
    ],
    'assets': {
        'web.assets_backend': [
            'looker_studio/static/src/js/patch_removefacet.js',
        ],
        'web.assets_web': [
            'looker_studio/static/src/js/patch_removefacet.js',
        ],
    },
    'installable': True,
    'application': False,
}
