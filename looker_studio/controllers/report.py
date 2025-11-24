from odoo import http
from odoo.http import request
from odoo.tools.safe_eval import safe_eval
import json


class LookerReportController(http.Controller):
    @http.route('/looker_studio/report/<int:report_id>', type='http', auth='user', website=True)
    def render_report(self, report_id, **kwargs):
        report = request.env['looker_studio.report'].sudo().browse(report_id)
        if not report.exists():
            return request.not_found()
        data = report.get_chart_data()
        # determine if success_domain is a valid non-empty domain list
        line_is_percentage = 0
        success_dom = []
        if report.success_domain:
            try:
                sd = safe_eval(report.success_domain) or []
                success_dom = sd
                line_is_percentage = 1 if sd else 0
            except Exception:
                line_is_percentage = 0

        # compute a few summary statistics for the small stats table
        Model = request.env['crm.lead'].sudo()
        domain = report._eval_domain()
        labels = data.get('labels', [])
        counts = data.get('count_values', [])
        sums = data.get('sum_values', [])
        total_leads = sum(counts) if counts else Model.search_count(domain)
        total_value = sum(sums) if sums else 0.0
        success_leads = 0
        try:
            if success_dom:
                success_leads = Model.search_count(list(domain) + list(success_dom))
            else:
                success_leads = 0
        except Exception:
            success_leads = 0
        success_pct = round((float(success_leads) / float(total_leads) * 100.0), 1) if total_leads else 0.0
        avg_value = round((float(total_value) / float(total_leads)), 2) if total_leads and total_value else 0.0

        # Calculate average Probability (Xác suất AI) per category for the line chart
        probability_values = []
        if report.group_field:
            try:
                group_field = report.group_field
                # Get all groups to match with labels
                all_groups = Model.read_group(domain, [group_field], [group_field], lazy=False)
                group_id_map = {}  # label -> group_id
                for g in all_groups:
                    key = g.get(group_field)
                    gid = key[0] if isinstance(key, (list, tuple)) else key
                    label = key[1] if isinstance(key, (list, tuple)) and len(key) > 1 else (key or 'Undefined')
                    group_id_map[str(label)] = gid
                
                # Calculate average probability for each category
                for i, label in enumerate(labels):
                    group_id = group_id_map.get(str(label))
                    if group_id:
                        # Build domain for this specific category
                        category_domain = list(domain) + [(group_field, '=', group_id)]
                        # Get all leads in this category
                        category_leads = Model.search(category_domain)
                        if category_leads:
                            # Calculate average probability
                            total_prob = sum(lead.probability or 0.0 for lead in category_leads)
                            avg_prob = total_prob / len(category_leads)
                            probability_values.append(round(avg_prob, 1))
                        else:
                            probability_values.append(0.0)
                    else:
                        probability_values.append(0.0)
            except Exception:
                probability_values = [0.0] * len(labels)
        else:
            # If no group_field, calculate overall average probability
            try:
                leads = Model.search(domain)
                if leads:
                    avg_prob = sum(lead.probability or 0.0 for lead in leads) / len(leads)
                    probability_values = [round(avg_prob, 1)]
                else:
                    probability_values = [0.0]
            except Exception:
                probability_values = [0.0] * len(labels) if labels else [0.0]

        context = {
            'report': report,
            'labels_json': json.dumps(labels),
            'counts_json': json.dumps(counts),
            'sums_json': json.dumps(sums),
            'line_labels_json': json.dumps(data.get('line_labels', [])),
            'line_values_json': json.dumps(data.get('line_values', [])),
            'line_is_percentage': line_is_percentage,
            'stat_total_leads': total_leads,
            'stat_success_leads': success_leads,
            'stat_success_pct': success_pct,
            'stat_total_value': total_value,
            'stat_avg_value': avg_value,
            'probability_json': json.dumps(probability_values),
            # Provide the json module to templates so they can call json.loads(...)
            'json': json,
            # Some simpler templates expect `values_json` (single dataset); expose counts as values
            'values_json': json.dumps(counts),
        }
        # Always render the modern 3-chart template on Preview (pie, line, bar)
        return request.render('looker_studio.report_modern_template', context)

    @http.route('/looker_studio/order_report/<int:report_id>', type='http', auth='user', website=True)
    def render_order_report(self, report_id, **kwargs):
        report = request.env['looker_studio.order_report'].sudo().browse(report_id)
        if not report.exists():
            return request.not_found()
        data = report.get_chart_data()

        Model = request.env['sale.order'].sudo()
        domain = report._eval_domain()
        labels = data.get('labels', [])
        counts = data.get('count_values', [])
        sums = data.get('sum_values', [])
        total_orders = sum(counts) if counts else Model.search_count(domain)
        total_amount = sum(sums) if sums else 0.0
        avg_amount = round((float(total_amount) / float(total_orders)), 2) if total_orders and total_amount else 0.0

        # probability_json not meaningful for orders; reuse field for a simple metric line if needed
        probability_values = data.get('line_values', [])

        context = {
            'report': report,
            'labels_json': json.dumps(labels),
            'counts_json': json.dumps(counts),
            'sums_json': json.dumps(sums),
            'line_labels_json': json.dumps(data.get('line_labels', [])),
            'line_values_json': json.dumps(data.get('line_values', [])),
            'line_is_percentage': 0,
            'stat_total_leads': total_orders,
            'stat_success_leads': 0,
            'stat_success_pct': 0.0,
            'stat_total_value': total_amount,
            'stat_avg_value': avg_amount,
            'probability_json': json.dumps(probability_values),
            'json': json,
            'values_json': json.dumps(counts),
        }
        # Render order-specific template with sales-appropriate labels
        return request.render('looker_studio.report_order_template', context)