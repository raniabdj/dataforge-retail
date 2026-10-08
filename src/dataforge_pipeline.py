"""DataForge: reproducible, incremental retail analytics pipeline (stdlib only)."""
import argparse, csv, datetime as dt, json, random, sqlite3
from collections import Counter
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
DATA=ROOT/'data'; OUT=ROOT/'outputs'; DB=DATA/'warehouse.sqlite'
BASE=dt.datetime(2026,1,1,tzinfo=dt.timezone.utc)

def generate(n=12000, seed=42):
    DATA.mkdir(exist_ok=True); r=random.Random(seed)
    customers=[{'customer_id':f'C{i:04d}','region':r.choice(['North','South','Midlands','Scotland','Wales']),'segment':r.choice(['Consumer','SMB','Enterprise'])} for i in range(1,601)]
    products=[{'product_id':f'P{i:03d}','category':r.choice(['Electronics','Home','Outdoor','Office']),'unit_cost':round(r.uniform(8,140),2)} for i in range(1,81)]
    with (DATA/'customers.csv').open('w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=customers[0]);w.writeheader();w.writerows(customers)
    with (DATA/'products.csv').open('w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=products[0]);w.writeheader();w.writerows(products)
    with (DATA/'events.jsonl').open('w') as f:
        for i in range(n):
            p=r.choice(products); event_time=BASE+dt.timedelta(minutes=i*18)
            status=r.choices(['completed','returned','cancelled'],[.87,.08,.05])[0]
            amount=round(p['unit_cost']*r.uniform(1.3,2.1)*r.randint(1,4),2)
            event={'event_id':f'E{i:07d}','order_id':f'O{i:07d}','customer_id':r.choice(customers)['customer_id'],'product_id':p['product_id'],'event_time':event_time.isoformat(),'ingested_at':(event_time+dt.timedelta(minutes=r.randint(1,120))).isoformat(),'status':status,'revenue':amount,'quantity':1}
            if i%499==0: event['revenue']=-20 # deliberately invalid
            f.write(json.dumps(event)+'\n')
            if i%997==0: f.write(json.dumps(event)+'\n') # deliberate duplicate
    return n

def connect():
    DATA.mkdir(exist_ok=True); c=sqlite3.connect(DB); c.row_factory=sqlite3.Row
    c.executescript('''PRAGMA foreign_keys=ON;
    CREATE TABLE IF NOT EXISTS raw_events(event_id TEXT PRIMARY KEY, payload TEXT NOT NULL, ingested_at TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS dim_customer(customer_id TEXT PRIMARY KEY, region TEXT, segment TEXT);
    CREATE TABLE IF NOT EXISTS dim_product(product_id TEXT PRIMARY KEY, category TEXT, unit_cost REAL);
    CREATE TABLE IF NOT EXISTS fact_orders(order_id TEXT PRIMARY KEY,event_id TEXT UNIQUE,customer_id TEXT,product_id TEXT,order_date TEXT,status TEXT,revenue REAL,quantity INTEGER,FOREIGN KEY(customer_id) REFERENCES dim_customer(customer_id),FOREIGN KEY(product_id) REFERENCES dim_product(product_id));
    CREATE TABLE IF NOT EXISTS rejected_events(event_id TEXT PRIMARY KEY,reason TEXT,payload TEXT);
    CREATE TABLE IF NOT EXISTS pipeline_runs(run_id INTEGER PRIMARY KEY AUTOINCREMENT,processed_at TEXT,received INTEGER,inserted INTEGER,duplicates INTEGER,rejected INTEGER);
    CREATE INDEX IF NOT EXISTS ix_fact_date ON fact_orders(order_date);
    CREATE INDEX IF NOT EXISTS ix_fact_customer ON fact_orders(customer_id);
    ''');return c

def run():
    OUT.mkdir(exist_ok=True); c=connect()
    for file,table,fields in [('customers.csv','dim_customer',['customer_id','region','segment']),('products.csv','dim_product',['product_id','category','unit_cost'])]:
        with (DATA/file).open() as f:
            rows=list(csv.DictReader(f))
        c.executemany(f"INSERT OR REPLACE INTO {table} VALUES ({','.join('?' for _ in fields)})",[[row[k] for k in fields] for row in rows])
    received=inserted=duplicates=rejected=0
    with (DATA/'events.jsonl').open() as f:
        for line in f:
            received+=1
            try:
                e=json.loads(line); eid=e['event_id']
                if c.execute('SELECT 1 FROM raw_events WHERE event_id=?',(eid,)).fetchone():duplicates+=1;continue
                c.execute('INSERT INTO raw_events VALUES(?,?,?)',(eid,json.dumps(e),e['ingested_at']))
                if e['revenue']<0 or e['quantity']<=0 or e['status'] not in ('completed','returned','cancelled'):
                    raise ValueError('invalid measure or status')
                if not c.execute('SELECT 1 FROM dim_customer WHERE customer_id=?',(e['customer_id'],)).fetchone():raise ValueError('unknown customer')
                if not c.execute('SELECT 1 FROM dim_product WHERE product_id=?',(e['product_id'],)).fetchone():raise ValueError('unknown product')
                c.execute('INSERT INTO fact_orders VALUES(?,?,?,?,?,?,?,?)',(e['order_id'],eid,e['customer_id'],e['product_id'],e['event_time'][:10],e['status'],e['revenue'],e['quantity']))
                inserted+=1
            except (ValueError,KeyError,TypeError) as ex:
                rejected+=1
                c.execute('INSERT OR REPLACE INTO rejected_events VALUES(?,?,?)',(e.get('event_id',f'line-{received}'),str(ex),line))
    c.execute('INSERT INTO pipeline_runs(processed_at,received,inserted,duplicates,rejected) VALUES(?,?,?,?,?)',(dt.datetime.now(dt.timezone.utc).isoformat(),received,inserted,duplicates,rejected))
    c.commit(); result={'received':received,'inserted':inserted,'duplicates':duplicates,'rejected':rejected,'fact_orders':c.execute('SELECT COUNT(*) FROM fact_orders').fetchone()[0]}
    (OUT/'run_summary.json').write_text(json.dumps(result,indent=2));export(c);c.close();return result

def export(c):
    for sqlfile in sorted((ROOT/'sql').glob('*.sql')):
        rows=c.execute(sqlfile.read_text()); names=[d[0] for d in rows.description]
        with (OUT/(sqlfile.stem+'.csv')).open('w',newline='') as f:
            w=csv.writer(f);w.writerow(names);w.writerows(rows.fetchall())

def check():
    c=connect(); checks={
      'unique_order_ids':c.execute('SELECT COUNT(*)=COUNT(DISTINCT order_id) FROM fact_orders').fetchone()[0]==1,
      'nonnegative_revenue':c.execute('SELECT COUNT(*)=0 FROM fact_orders WHERE revenue<0').fetchone()[0]==1,
      'valid_customer_fk':c.execute('SELECT COUNT(*)=0 FROM fact_orders f LEFT JOIN dim_customer d USING(customer_id) WHERE d.customer_id IS NULL').fetchone()[0]==1,
      'valid_product_fk':c.execute('SELECT COUNT(*)=0 FROM fact_orders f LEFT JOIN dim_product d USING(product_id) WHERE d.product_id IS NULL').fetchone()[0]==1,
      'has_rows':c.execute('SELECT COUNT(*)>0 FROM fact_orders').fetchone()[0]==1,
    };c.close();OUT.mkdir(exist_ok=True);(OUT/'quality_report.json').write_text(json.dumps(checks,indent=2));return checks

def main():
    p=argparse.ArgumentParser();p.add_argument('command',choices=['generate','run','check','all']);p.add_argument('--rows',type=int,default=12000);a=p.parse_args()
    if a.command in ('generate','all'):print('Generated',generate(a.rows),'events')
    if a.command in ('run','all'):print('Pipeline:',run())
    if a.command in ('check','all'):
        checks=check();print('Quality:',checks)
        if not all(checks.values()):raise SystemExit(1)
if __name__=='__main__':main()
