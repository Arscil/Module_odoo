from odoo import models, fields, api
from odoo.tools.safe_eval import safe_eval
from odoo.exceptions import UserError
import logging

_logger = logging.getLogger(__name__)


class LookerReport(models.Model):
    """Simple report record used by the Looker Studio module.

    The model always targets `crm.lead` as data source. This file focuses on
    generating chart-friendly aggregates and on small helpers that auto-fill
    user-facing descriptions in Vietnamese when they are not provided.
    """

    _name = 'looker_studio.report'
    _description = 'Looker Studio - Report (simple)'

    name = fields.Char(required=True)
    # Always use crm.lead as data source
    domain = fields.Text(string='Domain', help='Python literal list domain, e.g. [("stage_id","=","won")]')
    group_field = fields.Selection(selection='_get_crm_group_fields', string='Group By Field', help='CRM field to group by')
    value_field = fields.Selection(selection='_get_crm_value_fields', string='Value Field', help='Numeric CRM field to aggregate (sum)')
    chart_type = fields.Selection([('bar', 'Bar'), ('line', 'Line'), ('pie', 'Pie')], default='bar')
    limit = fields.Integer(string='Limit', default=1000)

    pie_description = fields.Text(string='Pie description', help='Short description displayed under the pie chart')
    line_description = fields.Text(string='Line description', help='Short description displayed under the line chart')
    bar_description = fields.Text(string='Bar description', help='Short description displayed under the bar chart')
    success_domain = fields.Text(string='Success Domain', help='Domain (Python list) selecting records considered "success" for percentage calculation, e.g. [("stage_id","=","won")]')

    # --- Auto-generation helpers for description fields ---
    def _crm_field_label(self, field_name):
        """Return the human label for a CRM field or the raw name as fallback."""
        if not field_name:
            return ''
        f = self.env['ir.model.fields'].sudo().search([('model', '=', 'crm.lead'), ('name', '=', field_name)], limit=1)
        return (f.field_description if f and f.field_description else field_name)

    def _build_pie_description(self):
        return ('Phân bố khách hàng tiềm năng theo %s.' % self._crm_field_label(self.group_field)) if self.group_field else 'Phân bố khách hàng tiềm năng.'

    def _build_bar_description(self):
        if self.group_field and self.value_field:
            return 'Tổng %s theo %s.' % (self._crm_field_label(self.value_field), self._crm_field_label(self.group_field))
        if self.group_field:
            return 'Số lượng khách hàng tiềm năng theo %s.' % (self._crm_field_label(self.group_field),)
        return 'Giá trị theo danh mục.'

    def _build_line_description(self):
        domain_part = ' (có áp dụng bộ lọc)' if self.domain else ''
        # If a success_domain is set, show success percentage trend
        if self.success_domain:
            return 'Xu hướng tỷ lệ phần trăm khách hàng tiềm năng theo ngày trong 14 ngày gần nhất%s.' % domain_part
        if self.value_field:
            return 'Xu hướng tổng %s theo ngày trong 14 ngày gần nhất%s.' % (self._crm_field_label(self.value_field), domain_part)
        return 'Xu hướng số lượng khách hàng tiềm năng trong 14 ngày gần nhất%s.' % domain_part

    @api.onchange('group_field', 'value_field', 'domain')
    def _onchange_auto_descriptions(self):
        for rec in self:
            if not rec.pie_description:
                rec.pie_description = rec._build_pie_description()
            if not rec.bar_description:
                rec.bar_description = rec._build_bar_description()
            if not rec.line_description:
                rec.line_description = rec._build_line_description()

    def _ensure_auto_descriptions(self, vals):
        # If any description is explicitly provided, do not overwrite provided ones
        if any(k in vals for k in ('pie_description', 'bar_description', 'line_description')):
            # still ensure missing keys are filled below
            pass

        # Prefer explicit values from vals
        group_field = vals.get('group_field')
        value_field = vals.get('value_field')
        domain = vals.get('domain')

        # If we are operating on an existing single record, use its values as fallback
        if not group_field and len(self) == 1 and self.exists():
            group_field = self.group_field or group_field
        if not value_field and len(self) == 1 and self.exists():
            value_field = self.value_field or value_field
        if not domain and len(self) == 1 and self.exists():
            domain = self.domain or domain

        def label(field):
            if not field:
                return ''
            f = self.env['ir.model.fields'].sudo().search([('model', '=', 'crm.lead'), ('name', '=', field)], limit=1)
            return (f.field_description if f and f.field_description else field)

        pie = ('Phân bố khách hàng tiềm năng theo %s.' % label(group_field)) if group_field else 'Phân bố khách hàng tiềm năng.'
        if group_field and value_field:
            bar = 'Tổng %s theo %s.' % (label(value_field), label(group_field))
        elif group_field:
            bar = 'Số lượng khách hàng tiềm năng theo %s.' % label(group_field)
        else:
            bar = 'Giá trị theo danh mục.'
        if value_field:
            line = 'Xu hướng tổng %s theo ngày trong 14 ngày gần nhất%s.' % (label(value_field), ' (có áp dụng bộ lọc)' if domain else '')
        else:
            line = 'Xu hướng số lượng khách hàng tiềm năng trong 14 ngày gần nhất%s.' % (' (có áp dụng bộ lọc)' if domain else '')

        # Only set keys that are absent
        if 'pie_description' not in vals:
            vals['pie_description'] = pie
        if 'bar_description' not in vals:
            vals['bar_description'] = bar
        if 'line_description' not in vals:
            vals['line_description'] = line
        return vals

    @api.model
    def create(self, vals):
        def ensure_and_validate(v):
            if not v.get('group_field') or not v.get('value_field'):
                raise UserError('Vui lòng chọn cả "Group By Field" và "Value Field" trước khi lưu báo cáo.')
            return self._ensure_auto_descriptions(v)

        if isinstance(vals, list):
            vals = [ensure_and_validate(v) for v in vals]
            return super(LookerReport, self).create(vals)
        vals = ensure_and_validate(vals)
        return super(LookerReport, self).create(vals)

    def write(self, vals):
        # Populate missing descriptions for updates when called with a dict
        if isinstance(vals, dict):
            vals = self._ensure_auto_descriptions(vals)
        return super(LookerReport, self).write(vals)

    def _eval_domain(self):
        if not self.domain:
            return []
        try:
            return safe_eval(self.domain)
        except Exception:
            return []

    @api.model
    def _get_crm_group_fields(self):
        """Return a selection of sensible group-by fields for crm.lead."""
        allowed = ['stage_id', 'user_id', 'team_id', 'partner_id', 'company_id', 'country_id']
        res = []
        for name in allowed:
            f = self.env['ir.model.fields'].sudo().search([('model', '=', 'crm.lead'), ('name', '=', name)], limit=1)
            if f:
                res.append((f.name, f.field_description or f.name))
        if not res:
            fields = self.env['ir.model.fields'].sudo().search([('model', '=', 'crm.lead')])
            for f in fields:
                if f.ttype in ('char', 'selection', 'many2one'):
                    res.append((f.name, f.field_description or f.name))
        return res

    @api.model
    def _get_crm_value_fields(self):
        """Return a selection of numeric fields usable as value metrics."""
        allowed = ['expected_revenue', 'planned_revenue', 'probability']
        res = []
        for name in allowed:
            f = self.env['ir.model.fields'].sudo().search([('model', '=', 'crm.lead'), ('name', '=', name)], limit=1)
            if f and f.ttype in ('integer', 'float', 'monetary'):
                res.append((f.name, f.field_description or f.name))
        if not res:
            fields = self.env['ir.model.fields'].sudo().search([('model', '=', 'crm.lead')])
            for f in fields:
                if f.ttype in ('integer', 'float', 'monetary'):
                    res.append((f.name, f.field_description or f.name))
        return res

    def get_chart_data(self):
        """Aggregate data for charts.

        Returns a dict with keys: labels, count_values, sum_values, line_labels, line_values.
        Always returns lists (never None) to simplify template handling.
        """
        self.ensure_one()
        Model = self.env['crm.lead']
        domain = self._eval_domain()

        labels = []
        count_values = []
        sum_values = []

        try:
            if self.group_field:
                try:
                    groups = Model.read_group(domain, [self.group_field], [self.group_field], lazy=False)
                except Exception:
                    _logger.exception('read_group(groups) failed for report %s', self.id)
                    groups = []

                sum_map = {}
                if self.value_field:
                    try:
                        grp_sum = Model.read_group(domain, [self.group_field, self.value_field], [self.group_field], lazy=False)
                    except Exception:
                        _logger.exception('read_group(sum) failed for report %s', self.id)
                        grp_sum = []
                    for g in grp_sum:
                        key = g.get(self.group_field)
                        gid = key[0] if isinstance(key, (list, tuple)) else key
                        sum_map[gid] = g.get(self.value_field) or 0.0

                group_entries = []
                for g in groups:
                    key = g.get(self.group_field)
                    gid = key[0] if isinstance(key, (list, tuple)) else key
                    lbl = key[1] if isinstance(key, (list, tuple)) and len(key) > 1 else (key or 'Undefined')
                    cnt = g.get('__count', 0)
                    sval = sum_map.get(gid, cnt if not self.value_field else 0.0)
                    group_entries.append({'gid': gid, 'label': str(lbl), 'count': cnt, 'sum': float(sval)})

                limit_n = int(self.limit) if getattr(self, 'limit', 0) and int(self.limit) > 0 else 0
                if limit_n and len(group_entries) > limit_n:
                    sort_key = 'sum' if self.value_field else 'count'
                    group_entries = sorted(group_entries, key=lambda x: x[sort_key], reverse=True)[:limit_n]

                for entry in group_entries:
                    labels.append(entry['label'])
                    count_values.append(entry['count'])
                    sum_values.append(entry['sum'])
            else:
                total = Model.search_count(domain)
                labels = ['All']
                count_values = [total]
                if self.value_field:
                    rows = Model.read_group(domain, [self.value_field], [], lazy=False)
                    total_sum = rows[0].get(self.value_field) if rows else 0.0
                    sum_values = [total_sum or 0.0]
                else:
                    sum_values = [total]

            # Time-series (last N days)
            from datetime import datetime, timedelta
            today = fields.Date.context_today(self)
            if isinstance(today, str):
                today_dt = datetime.strptime(today, '%Y-%m-%d')
            else:
                today_dt = datetime.combine(today, datetime.min.time())

            N = 14
            start_dt = (today_dt - timedelta(days=N - 1)).strftime('%Y-%m-%d 00:00:00')
            end_dt = (today_dt + timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')
            ts_domain = list(domain) + [('create_date', '>=', start_dt), ('create_date', '<', end_dt)]

            try:
                if self.value_field:
                    ts_groups = Model.read_group(ts_domain, ['create_date', self.value_field], ['create_date:day'], lazy=False)
                else:
                    ts_groups = Model.read_group(ts_domain, ['create_date'], ['create_date:day'], lazy=False)
            except Exception:
                _logger.exception('read_group(time-series) failed for report %s', self.id)
                ts_groups = []

            ts_map = {}
            ts_success_map = {}
            success_dom = []
            if self.success_domain:
                try:
                    success_dom = safe_eval(self.success_domain) or []
                except Exception:
                    success_dom = []

            for g in ts_groups:
                key = g.get('create_date')
                day = key[0] if isinstance(key, (list, tuple)) else key
                if self.value_field:
                    ts_map[str(day)] = g.get(self.value_field) or 0.0
                else:
                    ts_map[str(day)] = g.get('__count', 0)

            if success_dom:
                try:
                    ts_groups_success = Model.read_group(list(ts_domain) + list(success_dom), ['create_date'], ['create_date:day'], lazy=False)
                except Exception:
                    _logger.exception('read_group(time-series-success) failed for report %s', self.id)
                    ts_groups_success = []
                for g in ts_groups_success:
                    key = g.get('create_date')
                    day = key[0] if isinstance(key, (list, tuple)) else key
                    ts_success_map[str(day)] = g.get('__count', 0)

            line_labels = []
            line_values = []
            for i in range(N):
                day_dt = (today_dt - timedelta(days=N - 1 - i))
                day_str = day_dt.strftime('%Y-%m-%d')
                line_labels.append(day_str)
                if success_dom:
                    total = ts_map.get(day_str, 0)
                    succ = ts_success_map.get(day_str, 0)
                    perc = (float(succ) / float(total) * 100.0) if total else 0.0
                    line_values.append(round(perc, 1))
                else:
                    line_values.append(ts_map.get(day_str, 0 if not self.value_field else 0.0))

            return {
                'labels': labels,
                'count_values': count_values,
                'sum_values': sum_values,
                'line_labels': line_labels,
                'line_values': line_values,
            }
        except Exception:
            _logger.exception('Unexpected error in get_chart_data for report %s', getattr(self, 'id', '?'))
            return {'labels': [], 'count_values': [], 'sum_values': [], 'line_labels': [], 'line_values': []}

    def action_preview(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_url',
            'url': f'/looker_studio/report/{self.id}',
            'target': 'new',
        }


class LookerOrderReport(models.Model):
    """Report record targeting sale.order (Quotations / Orders).

    This parallel model keeps CRM reports unchanged while providing a focused
    report type for sale orders. It supports grouping by partner location
    (country/state), salesperson, and date-based dimensions (day, weekday).
    """

    _name = 'looker_studio.order_report'
    _description = 'Looker Studio - Order Report (sale.order)'

    name = fields.Char(required=True)
    domain = fields.Text(string='Domain', help='Python literal list domain, e.g. [("state","=","sale")])')
    group_field = fields.Selection(selection='_get_order_group_fields', string='Group By Field', help='Sale order field to group by')
    value_field = fields.Selection(selection='_get_order_value_fields', string='Value Field', help='Numeric sale.order field to aggregate (sum)')
    chart_type = fields.Selection([('bar', 'Bar'), ('line', 'Line'), ('pie', 'Pie')], default='bar')
    limit = fields.Integer(string='Limit', default=1000)

    pie_description = fields.Text(string='Pie description')
    line_description = fields.Text(string='Line description')
    bar_description = fields.Text(string='Bar description')

    def _order_field_label(self, field_name):
        """Return human label for a sale.order field or the raw name as fallback."""
        if not field_name:
            return ''
        # Support relational dotted names like partner_id.country_id
        if isinstance(field_name, str) and '.' in field_name:
            parts = field_name.split('.')
            top = parts[0]
            sub = parts[1] if len(parts) > 1 else None
            # Try to resolve top field description
            f_top = self.env['ir.model.fields'].sudo().search([('model', '=', 'sale.order'), ('name', '=', top)], limit=1)
            top_label = f_top.field_description if f_top and f_top.field_description else top
            if sub and f_top and f_top.ttype == 'many2one' and f_top.relation:
                f_sub = self.env['ir.model.fields'].sudo().search([('model', '=', f_top.relation), ('name', '=', sub)], limit=1)
                sub_label = f_sub.field_description if f_sub and f_sub.field_description else sub
                # Return e.g. "Quốc gia (Đối tác)"
                return '%s (%s)' % (sub_label, top_label)
            # Fallback: return the dotted name but replace '_' with ' '
            return field_name.replace('_', ' ')
        # Non-dotted field: look up directly on sale.order
        f = self.env['ir.model.fields'].sudo().search([('model', '=', 'sale.order'), ('name', '=', field_name)], limit=1)
        return (f.field_description if f and f.field_description else (field_name or ''))

    def _build_pie_description(self):
        return ('Phân bố đơn hàng theo %s.' % self._order_field_label(self.group_field)) if self.group_field else 'Phân bố đơn hàng.'

    def _build_bar_description(self):
        if self.group_field and self.value_field:
            val_label = self._order_field_label(self.value_field)
            # avoid duplicated leading words like 'Tổng' -> produce 'Tổng Giá trị theo ...' instead of 'Tổng Tổng ...'
            sval = val_label.strip()
            if sval.lower() == 'tổng':
                sval = 'giá trị'
            elif sval.lower().startswith('tổng '):
                sval = sval[5:].strip()
            return 'Tổng %s theo %s.' % (sval, self._order_field_label(self.group_field))
        if self.group_field:
            return 'Số lượng đơn hàng theo %s.' % (self._order_field_label(self.group_field),)
        return 'Giá trị theo danh mục.'

    def _build_line_description(self):
        domain_part = ' (có áp dụng bộ lọc)' if self.domain else ''
        if self.value_field:
            val_label = self._order_field_label(self.value_field)
            sval = val_label.strip()
            if sval.lower() == 'tổng':
                sval = 'giá trị'
            elif sval.lower().startswith('tổng '):
                sval = sval[5:].strip()
            return 'Xu hướng tổng %s theo ngày trong 14 ngày gần nhất%s.' % (sval, domain_part)
        return 'Xu hướng số lượng đơn hàng trong 14 ngày gần nhất%s.' % domain_part

    @api.onchange('group_field', 'value_field', 'domain')
    def _onchange_auto_descriptions(self):
        for rec in self:
            if not rec.pie_description:
                rec.pie_description = rec._build_pie_description()
            if not rec.bar_description:
                rec.bar_description = rec._build_bar_description()
            if not rec.line_description:
                rec.line_description = rec._build_line_description()

    def _ensure_auto_descriptions(self, vals):
        # Similar behavior to LookerReport: populate missing description keys
        group_field = vals.get('group_field')
        value_field = vals.get('value_field')
        domain = vals.get('domain')

        if not group_field and len(self) == 1 and self.exists():
            group_field = self.group_field or group_field
        if not value_field and len(self) == 1 and self.exists():
            value_field = self.value_field or value_field
        if not domain and len(self) == 1 and self.exists():
            domain = self.domain or domain

        pie = ('Phân bố đơn hàng theo %s.' % self._order_field_label(group_field)) if group_field else 'Phân bố đơn hàng.'
        if group_field and value_field:
            bar = 'Tổng %s theo %s.' % (self._order_field_label(value_field), self._order_field_label(group_field))
        elif group_field:
            bar = 'Số lượng đơn hàng theo %s.' % self._order_field_label(group_field)
        else:
            bar = 'Giá trị theo danh mục.'
        if value_field:
            line = 'Xu hướng tổng %s theo ngày trong 14 ngày gần nhất%s.' % (self._order_field_label(value_field), ' (có áp dụng bộ lọc)' if domain else '')
        else:
            line = 'Xu hướng số lượng đơn hàng trong 14 ngày gần nhất%s.' % (' (có áp dụng bộ lọc)' if domain else '')

        if 'pie_description' not in vals:
            vals['pie_description'] = pie
        if 'bar_description' not in vals:
            vals['bar_description'] = bar
        if 'line_description' not in vals:
            vals['line_description'] = line
        return vals

    def create(self, vals):
        def ensure_and_validate(v):
            if not v.get('group_field') or not v.get('value_field'):
                raise UserError('Vui lòng chọn cả "Group By Field" và "Value Field" trước khi lưu báo cáo.')
            return self._ensure_auto_descriptions(v)

        if isinstance(vals, list):
            vals = [ensure_and_validate(v) for v in vals]
            return super(LookerOrderReport, self).create(vals)
        vals = ensure_and_validate(vals)
        return super(LookerOrderReport, self).create(vals)

    def write(self, vals):
        if isinstance(vals, dict):
            vals = self._ensure_auto_descriptions(vals)
        return super(LookerOrderReport, self).write(vals)

    @api.model
    def _get_order_group_fields(self):
        # prefer partner.city for province/municipality grouping (customer 'city' field)
        allowed = ['partner_id', 'partner_id.country_id', 'partner_id.city', 'user_id', 'date_order', 'date_order_weekday']
        res = []
        for name in allowed:
            # Try to find a field when it's a direct field on sale.order
            if '.' not in name:
                f = self.env['ir.model.fields'].sudo().search([('model', '=', 'sale.order'), ('name', '=', name)], limit=1)
                if f:
                    res.append((f.name, f.field_description or f.name))
                else:
                    # For computed helpers like date_order_weekday add a friendly label
                    if name == 'date_order_weekday':
                        res.append((name, 'Ngày trong tuần'))
            else:
                # For relational subfields, present a friendly label
                if name == 'partner_id.country_id':
                    res.append((name, 'Quốc gia (Đối tác)'))
                elif name == 'partner_id.city':
                    res.append((name, 'Tỉnh/Thành (Đối tác)'))
        # Fallback: scan sale.order fields for common types
        if not res:
            fields = self.env['ir.model.fields'].sudo().search([('model', '=', 'sale.order')])
            for f in fields:
                if f.ttype in ('char', 'selection', 'many2one'):
                    res.append((f.name, f.field_description or f.name))
        return res

    @api.model
    def _get_order_value_fields(self):
        # Prefer common sale.order monetary fields present on most databases
        allowed = ['amount_total', 'amount_untaxed', 'amount_tax']
        res = []
        for name in allowed:
            f = self.env['ir.model.fields'].sudo().search([('model', '=', 'sale.order'), ('name', '=', name)], limit=1)
            if f and f.ttype in ('integer', 'float', 'monetary'):
                res.append((f.name, f.field_description or f.name))
        if not res:
            fields = self.env['ir.model.fields'].sudo().search([('model', '=', 'sale.order')])
            for f in fields:
                if f.ttype in ('integer', 'float', 'monetary'):
                    res.append((f.name, f.field_description or f.name))
        return res

    def _eval_domain(self):
        if not self.domain:
            return []
        try:
            return safe_eval(self.domain)
        except Exception:
            return []

    def get_chart_data(self):
        self.ensure_one()
        Model = self.env['sale.order']
        domain = self._eval_domain()

        labels = []
        count_values = []
        sum_values = []

        try:
            # Special support: group by weekday
            if self.group_field == 'date_order_weekday':
                # build weekday buckets Mon..Sun
                from datetime import datetime
                weekday_map = {i: 0 for i in range(7)}
                sum_map = {i: 0.0 for i in range(7)}
                rows = Model.search(domain)
                for r in rows:
                    dt = r.date_order
                    if not dt:
                        continue
                    if isinstance(dt, str):
                        try:
                            dt_obj = datetime.strptime(dt, '%Y-%m-%d %H:%M:%S')
                        except Exception:
                            continue
                    else:
                        dt_obj = dt
                    wd = dt_obj.weekday()
                    weekday_map[wd] += 1
                    if self.value_field:
                        sum_map[wd] += float(getattr(r, self.value_field) or 0.0)
                days = ['Thứ 2','Thứ 3','Thứ 4','Thứ 5','Thứ 6','Thứ 7','Chủ Nhật']
                for i in range(7):
                    labels.append(days[i])
                    count_values.append(weekday_map.get(i, 0))
                    sum_values.append(round(sum_map.get(i, 0.0), 2))
            elif self.group_field and '.' in str(self.group_field):
                # relational subgroup like partner_id.state_id or partner_id.country_id
                parts = str(self.group_field).split('.')
                top = parts[0]
                rel_field = parts[1] if len(parts) > 1 else None
                try:
                    groups = Model.read_group(domain, [top], [top], lazy=False)
                except Exception:
                    groups = []

                # optional per-top sums when value_field is provided
                sum_map_per_top = {}
                if self.value_field:
                    try:
                        grp_sum = Model.read_group(domain, [top, self.value_field], [top], lazy=False)
                    except Exception:
                        grp_sum = []
                    for g in grp_sum:
                        key = g.get(top)
                        tg = key[0] if isinstance(key, (list, tuple)) else key
                        sum_map_per_top[tg] = g.get(self.value_field) or 0.0

                # determine relation model for the top field (most often many2one)
                rel_model = None
                try:
                    f_top = self.env['ir.model.fields'].sudo().search([('model', '=', 'sale.order'), ('name', '=', top)], limit=1)
                    if f_top and f_top.ttype == 'many2one':
                        rel_model = f_top.relation
                except Exception:
                    rel_model = None

                buckets = {}
                for g in groups:
                    key = g.get(top)
                    gid = key[0] if isinstance(key, (list, tuple)) else key
                    cnt = g.get('__count', 0)
                    # find related target (e.g., partner.country_id)
                    if not gid:
                        rel_id = None
                        rel_label = 'Undefined'
                        rel_sum = 0.0
                    else:
                        rel_sum = sum_map_per_top.get(gid, 0.0)
                        if rel_model:
                            try:
                                rel_rec = self.env[rel_model].sudo().browse(gid)
                                target = getattr(rel_rec, rel_field, None)
                                if target:
                                    rel_id = target.id if hasattr(target, 'id') else target
                                    rel_label = (target.name if hasattr(target, 'name') else str(target)) or 'Undefined'
                                else:
                                    rel_id = None
                                    rel_label = 'Undefined'
                            except Exception:
                                rel_id = None
                                rel_label = 'Undefined'
                        else:
                            # fallback: cannot resolve relation model, use top label
                            rel_id = gid
                            rel_label = (key[1] if isinstance(key, (list, tuple)) and len(key) > 1 else (key or 'Undefined'))

                    if rel_id not in buckets:
                        buckets[rel_id] = {'gid': rel_id, 'label': str(rel_label), 'count': 0, 'sum': 0.0}
                    buckets[rel_id]['count'] += int(cnt or 0)
                    buckets[rel_id]['sum'] += float(rel_sum or 0.0)

                # convert buckets to entries
                group_entries = []
                for b in buckets.values():
                    group_entries.append({'gid': b['gid'], 'label': b['label'], 'count': b['count'], 'sum': float(b['sum'])})

                # Apply Top-N limit if requested (same logic as other branch)
                limit_n = int(self.limit) if getattr(self, 'limit', 0) and int(self.limit) > 0 else 0
                if limit_n and len(group_entries) > limit_n:
                    sort_key = 'sum' if self.value_field else 'count'
                    group_entries = sorted(group_entries, key=lambda x: x[sort_key], reverse=True)[:limit_n]

                # Populate output arrays from entries
                for entry in group_entries:
                    labels.append(entry['label'])
                    count_values.append(entry['count'])
                    sum_values.append(entry['sum'])
            elif self.group_field:
                try:
                    groups = Model.read_group(domain, [self.group_field], [self.group_field], lazy=False)
                except Exception:
                    groups = []
                # If a value_field is requested, compute per-group sums using read_group
                sum_map = {}
                if self.value_field:
                    try:
                        grp_sum = Model.read_group(domain, [self.group_field, self.value_field], [self.group_field], lazy=False)
                    except Exception:
                        grp_sum = []
                    for gs in grp_sum:
                        key = gs.get(self.group_field)
                        gid = key[0] if isinstance(key, (list, tuple)) else key
                        sum_map[gid] = gs.get(self.value_field) or 0.0

                for g in groups:
                    key = g.get(self.group_field)
                    gid = key[0] if isinstance(key, (list, tuple)) else key
                    lbl = key[1] if isinstance(key, (list, tuple)) and len(key) > 1 else (key or 'Undefined')
                    cnt = g.get('__count', 0)
                    sval = sum_map.get(gid, cnt if not self.value_field else 0.0)
                    labels.append(str(lbl))
                    count_values.append(cnt)
                    sum_values.append(float(sval or 0.0))
            else:
                total = Model.search_count(domain)
                labels = ['All']
                count_values = [total]
                if self.value_field:
                    rows = Model.read_group(domain, [self.value_field], [], lazy=False)
                    total_sum = rows[0].get(self.value_field) if rows else 0.0
                    sum_values = [total_sum or 0.0]
                else:
                    sum_values = [total]

            # Basic time-series: last 14 days by create_date
            from datetime import datetime, timedelta
            today = fields.Date.context_today(self)
            if isinstance(today, str):
                today_dt = datetime.strptime(today, '%Y-%m-%d')
            else:
                today_dt = datetime.combine(today, datetime.min.time())
            N = 14
            start_dt = (today_dt - timedelta(days=N - 1)).strftime('%Y-%m-%d 00:00:00')
            end_dt = (today_dt + timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')
            ts_domain = list(domain) + [('create_date', '>=', start_dt), ('create_date', '<', end_dt)]
            try:
                if self.value_field:
                    ts_groups = Model.read_group(ts_domain, ['create_date', self.value_field], ['create_date:day'], lazy=False)
                else:
                    ts_groups = Model.read_group(ts_domain, ['create_date'], ['create_date:day'], lazy=False)
            except Exception:
                ts_groups = []
            ts_map = {}
            for g in ts_groups:
                key = g.get('create_date')
                day = key[0] if isinstance(key, (list, tuple)) else key
                if self.value_field:
                    ts_map[str(day)] = g.get(self.value_field) or 0.0
                else:
                    ts_map[str(day)] = g.get('__count', 0)

            line_labels = []
            line_values = []
            for i in range(N):
                day_dt = (today_dt - timedelta(days=N - 1 - i))
                day_str = day_dt.strftime('%Y-%m-%d')
                line_labels.append(day_str)
                line_values.append(ts_map.get(day_str, 0 if not self.value_field else 0.0))

            return {
                'labels': labels,
                'count_values': count_values,
                'sum_values': sum_values,
                'line_labels': line_labels,
                'line_values': line_values,
            }
        except Exception:
            _logger.exception('Unexpected error in get_chart_data for order report %s', getattr(self, 'id', '?'))
            return {'labels': [], 'count_values': [], 'sum_values': [], 'line_labels': [], 'line_values': []}

    def action_preview(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_url',
            'url': f'/looker_studio/order_report/{self.id}',
            'target': 'new',
        }
