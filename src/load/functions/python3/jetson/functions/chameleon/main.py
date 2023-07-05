msg = "good"
import traceback
try:
  from time import time
  import six
  import json
  from chameleon import PageTemplate
except Exception as e:
    msg = traceback.format_exc()

BIGTABLE_ZPT = """\
<table xmlns="http://www.w3.org/1999/xhtml"
xmlns:tal="http://xml.zope.org/namespaces/tal">
<tr tal:repeat="row python: options['table']">
<td tal:repeat="c python: row.values()">
<span tal:define="d python: c + 1"
tal:attributes="class python: 'column-' + %s(d)"
tal:content="python: d" />
</td>
</tr>
</table>""" % six.text_type.__name__

cold = True

def main(args):
    global cold
    was_cold = cold
    cold = False
    num_of_rows = int(args.get("num_of_rows", 10))
    num_of_cols = int(args.get("num_of_cols", 10))

    start = time()
    latency = start
    data = {}
    try:
      tmpl = PageTemplate(BIGTABLE_ZPT)

      for i in range(num_of_cols):
          data[str(i)] = i

      table = [data for x in range(num_of_rows)]
      options = {'table': table}

      data = tmpl.render(options=options)
      end = time()
      latency = end - start
      return {"body": {'latency': latency, 'data': data, "cold":was_cold, "start":start, "end":end}}
    except Exception as e:
      err = "whelp"
      try:
          err = traceback.format_exc()
      except Exception as fug:
          err = str(fug)
      return {"body": {"cust_error":msg, "thing":err, "cold":was_cold}}