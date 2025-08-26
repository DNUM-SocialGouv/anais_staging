-- Renvoie soit la date fournie (YYYY-MM-DD) parsée en date, soit la date du jour (type: datetime.date)
{% macro get_reference_date(reference_date) %}
  {% if reference_date is not none %}
    {% set ref_date = modules.datetime.datetime.strptime(reference_date, '%Y-%m-%d').date() %}
  {% else %}
    {% set ref_date = modules.datetime.date.today() %}
  {% endif %}
  {{ return(ref_date) }}
{% endmacro %}

-- Renvoie l'année précédente (ou l'année - i)
{% macro get_previous_year(i=1, reference_date=None) %}
  {% set ref_year = dbtStaging.get_reference_date(reference_date).year %}
    {{ (ref_year - i) }}
{% endmacro %}


-- Renvoie les x années précédentes sous la forme ('YYYY', ... ,'YYYY')
{% macro get_x_previous_year(x=3, reference_date=None) %}
  {% set ref_year = dbtStaging.get_reference_date(reference_date).year %}
  (
      {%- for i in range(x, 0, -1) -%}
          {{ (ref_year - i) }}
          {%- if not loop.last %}, {% endif %}
      {%- endfor -%}
  )
{% endmacro %}

-- Renvoie le dernier mois de l'année précédente 'YYYYmm'
{% macro get_last_month_of_last_year(reference_date=None) %}
  {% set ref_date = dbtStaging.get_reference_date(reference_date) %}
  {{ (ref_date.year - 1) | string ~ '12' }}
{% endmacro %}

-- Renvoie la première date d'il y a x année 'YYYY-mm-dd'
{% macro get_first_day_of_x_years_ago(x=3, reference_date=None) %}
  {% set ref_date = dbtStaging.get_reference_date(reference_date) %}
  {{ (ref_date.year - x) | string ~ '-01-01' }}
{% endmacro %}

-- Renvoie la date du jour précédent 'YYYY-mm-dd'
{% macro get_yesterday(reference_date=None) %}
  {% set ref_date = dbtStaging.get_reference_date(reference_date) %}
  {{ ref_date - modules.datetime.timedelta(days=1) }}
{% endmacro %}

-- Renvoie les 6 derniers mois de l'année en cours 
{% macro where_last_6_months(reference_date=None, date_column_year='Annee', date_column_month='MOIS') %}
  {% set ref_date = dbtStaging.get_reference_date(reference_date) %}

    {% set months_back = [] %}
    {% set year = ref_date.year %}
    {% set month = ref_date.month %}

    {% for i in range(6) %}
        {% set current_month = month - i %}
        {% if current_month <= 0 %}
            {% set m = current_month + 12 %}
            {% set y = year - 1 %}
        {% else %}
            {% set m = current_month %}
            {% set y = year %}
        {% endif %}
        {% do months_back.append((y, m)) %}
    {% endfor %}

    (
        {% for ym in months_back %}
            ({{ date_column_year }}::INT = {{ ym[0] }} AND {{ date_column_month }}::INT = {{ ym[1] }})
            {% if not loop.last %} OR {% endif %}
        {% endfor %}
    )
{% endmacro %}

-- Renvoie la date de début de l'année glissante (date d'il y a un an)
{% macro get_first_date_last_rolling_years(reference_date=None) %}
  {% set ref_date = dbtStaging.get_reference_date(reference_date) %}
    {{ return(ref_date.replace(year = ref_date.year - 1).strftime('%Y%m%d') | int) }}
{% endmacro %}

-- Renvoie la date de fin de l'année glissante
{% macro get_last_date_last_rolling_years(reference_date=None) %}
  {% set ref_date = get_yesterday(reference_date) %}
    {{ return(ref_date.strftime('%Y%m%d') | int) }}
{% endmacro %}

-- Renvoie les mois d'une année qui ne font pas parties des 6 derniers mois glissants
{% macro where_remaining_last_year_months(reference_date=None, date_column_year='annee', date_column_month='mois') %}
  {% set ref_date = dbtStaging.get_reference_date(reference_date) %}

    {% set year = ref_date.year %}
    {% set month = ref_date.month %}

    {# Calcul du mois de début des 6 derniers mois glissants #}
    {% set sliding_start_month = month - 5 %}
    {% set sliding_start_year = year %}
    {% if sliding_start_month <= 0 %}
        {% set sliding_start_month = sliding_start_month + 12 %}
        {% set sliding_start_year = year - 1 %}
    {% endif %}

    {% set months_to_include = [] %}
    {% for m in range(1, 13) %}
        {% if m < sliding_start_month %}
            {% do months_to_include.append(m) %}
        {% endif %}
    {% endfor %}

    (
        {{ date_column_year }}::INT = {{ sliding_start_year }}
        AND {{ date_column_month }}::INT IN (
            {{ months_to_include | join(', ') }}
        )
    )
{% endmacro %}

-- Renvoie l'année et le mois relatif au nombre de mois d'écart i souhaité. Sous forme adec = YYYY AND mdec = mm
{% macro previous_month_condition(reference_date=None, i=1, year_col='adec', month_col='mdec') %}
  {% set ref_date = dbtStaging.get_reference_date(reference_date) %}
  
  {# Calcul du nouveau mois en reculant de i mois #}
  {% set month = ref_date.month - i %}
  {% set year = ref_date.year %}
  
  {% if month <= 0 %}
    {% set month = month + 12 %}
    {% set year = year - 1 %}
  {% endif %}
  
  {% set condition = year_col ~ '::INT = ' ~ year ~ ' AND ' ~ month_col ~ '::INT = ' ~ month %}
  
  {{ return(condition) }}
{% endmacro %}

-- Renvoie l'année précédente complète et l'année en cours (en dehors de juillet)
{% macro previous_year_and_current(reference_date=None, year_col='adec', month_col='mdec') %}
    {% set ref_date = dbtStaging.get_reference_date(reference_date) %}
    {% set month = ref_date.month %}
    {% set year = ref_date.year %}
    {% set condition = '(' ~ year_col ~ '::INT = ' ~ (year - 1) | string ~
                        ' OR (' ~ year_col ~ '::INT = ' ~ year | string ~
                        ' AND ' ~ month_col ~ '::INT < ' ~ month | string ~ 
                        ' AND ' ~ month_col ~ '::INT !=7))' %}
    {{ return(condition) }}
{% endmacro %}