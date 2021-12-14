import asyncio
from elasticsearch import AsyncElasticsearch
from elastic_query import toktok_elastic
from fastapi import FastAPI
from typing import Optional

comm_index = 'toktok_op_talk*'
assign_index = 'toktok_op_assignable_regist*'
submit_index = 'toktok_op_assignable_submit*'

hostname = 'datamgt9200.itt.link'
port = 80
id = 'publicai'
password = 'publicai@@!!'

app = FastAPI()
tok_es = toktok_elastic()
es = AsyncElasticsearch([{'host' : hostname, 'port' : port}], http_auth = (id, password), timeout=30)

@app.get("/comm_index_per_student/")
async def comm_index_per_student(cmmnty_id:str, start_date:str, end_date:str):
    query = tok_es.comm_index_per_student(cmmnty_id, start_date, end_date)
    result = await es.search(
        index=comm_index, body=query
    )
    return result['aggregations']

@app.get("/comm_index_per_class/")
async def comm_index_per_class(start_date:str, end_date:str):
    query = tok_es.comm_index_per_class(start_date, end_date)
    result = await es.search(
        index=comm_index, body=query
    )
    return result['aggregations']

@app.get("/submit_rate_by_student/")
async def submit_rate_by_student(cmmnty_id:str, student_id:str, start_date:str, end_date:str):
    submit_query, assign_query = tok_es.submit_rate_by_student(cmmnty_id, student_id, start_date, end_date)

    submit_result = await es.search(
        index=submit_index, body=submit_query
    )
    submit_count = submit_result['aggregations']['submit_count']['value']

    assign_result = await es.search(
        index=assign_index, body=assign_query
    )
    assign_count = assign_result['aggregations']['assign_count']['value']

    if assign_count == 0:
        submit_rate = 0
    else:
        submit_rate = submit_count / assign_count

    return "{:.2%}".format(submit_rate)
#asyncio.run(comm_index_per_student(index, _cmmnty_id, _start_date, _end_date))