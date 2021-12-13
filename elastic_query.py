from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, A, Q


## TODO : print 여부 파라미터 추가
## TODO : size 추가 여부 확인할 것 (속도에 따라)
## TODO : 출제 과제 리스트 계산 여부 확인
## TODO : null 처리 확인

hostname = 'datamgt9200.itt.link'
port = 80
id = 'publicai'
password = 'publicai@@!!'

comm_index = 'toktok_op_talk*'
assign_index = 'toktok_op_assignable_regist*'
submit_index = 'toktok_op_assignable_submit*'

class toktok_elastic(object):
    def __init__(self, hostname=hostname, port=port, id=id, password=password):
        self.hostname = hostname
        self.port = port
        self.id = id
        self.password = password
        self.es = Elasticsearch([{'host' : hostname, 'port' : port}], http_auth = (id, password), timeout=30)

    def comm_index_per_class(self, start_date, end_date, date_interval='week', normalize_method='rescale_0_1'):
        self.index = comm_index
        s = Search(using=self.es, index=self.index)
        ## filtering data
        s = s.query('bool', filter=[Q('range', post_frst_register_pnttm= \
                {'gte' : f"{start_date}T00:00:00.000Z", 'lt' : f"{end_date}T00:00:00.000Z"})])
        ## aggregating data
        s.aggs.bucket('comm_over_date', 'date_histogram',
                      field='post_frst_register_pnttm', interval=date_interval) \
            .bucket('comm_per_class', 'terms', field='cmmnty_id') \
            .metric('comment', 'value_count', field='comment_frst_register_pnttm') \
            .pipeline('minmax_comment', 'normalize', buckets_path='comment', method=normalize_method, format='00.00%') \
            .metric('reply', 'value_count', field='reply_frst_register_pnttm') \
            .pipeline('minmax_reply', 'normalize', buckets_path='reply', method=normalize_method, format='00.00%') \
            .metric('good', 'value_count', field='good_frst_register_pnttm') \
            .pipeline('minmax_good', 'normalize', buckets_path='good', method=normalize_method, format='00.00%') \
            .pipeline('comm_index', 'bucket_script', buckets_path={'comment': 'minmax_comment',
                                                                   'reply': 'minmax_reply',
                                                                   'good': 'minmax_good'},
                      script={'source': 'params.comment + params.reply + params.good'})
        print(s.to_dict())
        ## commit query
        response = s.execute()
        print(f'{response.took} (ms) 소요')
        return response.aggregations.to_dict()

    def comm_index_per_student(self, cmmnty_id, start_date, end_date, date_interval='week', normalize_method='rescale_0_1'):
        self.index = comm_index
        s = Search(using=self.es, index=self.index)
        ## filtering data
        s = s.query("bool", filter=[Q('term', cmmnty_id={'value': cmmnty_id}),
                                    Q('range', post_frst_register_pnttm= \
                                        {'gte': f"{start_date}T00:00:00.000Z", 'lt': f"{end_date}T00:00:00.000Z"})])
        ## aggregating data
        s.aggs.bucket('comm_over_date', 'date_histogram',
                      field='post_frst_register_pnttm', interval=date_interval) \
            .bucket('comm_per_student', 'terms', field='post_frst_register_id.keyword') \
            .metric('comment', 'value_count', field='comment_frst_register_pnttm') \
            .pipeline('minmax_comment', 'normalize', buckets_path='comment', method=normalize_method, format='00.00%') \
            .metric('reply', 'value_count', field='reply_frst_register_pnttm') \
            .pipeline('minmax_reply', 'normalize', buckets_path='reply', method=normalize_method, format='00.00%') \
            .metric('good', 'value_count', field='good_frst_register_pnttm') \
            .pipeline('minmax_good', 'normalize', buckets_path='good', method=normalize_method, format='00.00%') \
            .pipeline('comm_index', 'bucket_script', buckets_path={'comment': 'minmax_comment',
                                                                   'reply': 'minmax_reply',
                                                                   'good': 'minmax_good'},
                      script={'source': 'params.comment + params.reply + params.good'})
        print(s.to_dict())
        ## commit query
        response = s.execute()
        print(f'{response.took} (ms) 소요')
        return response.aggregations.to_dict()

    def submit_rate_by_student(self, cmmnty_id, student_id, start_date, end_date):
        self.submit_index = submit_index
        self.assign_index = assign_index

        # calculate submit count
        s = Search(using=self.es, index=self.submit_index)
        ## filtering data
        s = s.query("bool", filter=[Q('term', cmmnty_id={'value': cmmnty_id}),
                                    Q('term', last_updusr_id={'value': student_id}),
                                    Q('range', last_updusr_pnttm= \
                                        {'gte': f"{start_date}T00:00:00.000Z", 'lt': f"{end_date}T00:00:00.000Z"})])
        ## aggregating data
        s.aggs.metric('submit_per_student', 'terms', field='last_updusr_id') \
            .metric('submit_count', 'value_count', field='ntt_no')
        print(s.to_dict())
        ## commit query
        submit_res = s.execute()
        print(f'{submit_res.took} (ms) 소요')
        submit_count = submit_res.aggregations.submit_count['value']

        # calculate assign count
        s = Search(using=self.es, index=self.assign_index)
        ## filtering data
        s = s.query("bool", filter=[Q('term', cmmnty_id={'value': cmmnty_id}),
                                    Q('range', assignable_end_date= \
                                        {'gte': f"{start_date}T00:00:00.000Z", 'lt': f"{end_date}T00:00:00.000Z"})])
        ## aggregating data
        s.aggs.metric('assign_per_class', 'terms', field='cmmnty_id') \
            .metric('assign_count', 'value_count', field='ntt_no')
        print(s.to_dict())

        ## commit query
        assign_res = s.execute()
        print(f'{assign_res.took} (ms) 소요')
        assign_count = assign_res.aggregations.assign_count['value']

        # assign list 추출
        self.assign_list = []
        for hit in assign_res.hits:
            self.assign_list.append(hit.to_dict())

        def print_assign_list(self):
            return self.assign_list

        return submit_count / assign_count
