import json, os
src = os.path.expanduser("~/Downloads/today_hn.json")
tmpl = os.path.expanduser("~/Downloads/hn_reader_template.html")
out = os.path.expanduser("~/Downloads/hn_reader_live.html")
if not os.path.exists(src):
    print("HN Reader: no stories yet for today")
else:
    stories = [json.loads(l) for l in open(src) if l.strip()]
    html = open(tmpl).read().replace("__STORIES_DATA__", json.dumps(stories))
    open(out, "w").write(html)
    print(f"HN Reader: {len(stories)} stories ready")
