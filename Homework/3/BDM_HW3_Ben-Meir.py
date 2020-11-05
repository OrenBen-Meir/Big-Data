import mrjob
from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
import datetime
import json

OUTPUT_HEADER = ','.join(['Product','Year','Number of Complaints','Number of Companies','Highest Percentage of Total Complaints Against One Company'])

class MRConsumerComplaints(MRJob):
    OUTPUT_PROTOCOL = mrjob.protocol.RawValueProtocol
        
    # This is supposed to help handlec multiline rows and cells that have commas
    def mapper_raw1_csv_dictreader(self, csv_path, csv_uri): 
        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f)
            for line in reader:
                yield reader.line_num, line
    
    def mapper2_productyear_complaint(self, _, line):
        row = dict(line)
        year = datetime.datetime.strptime(row['Date received'], '%Y-%m-%d').year
        yield (row['Product'].lower(), year), row['Company']
    
    def reducer2_productyear_complaints(self, productyear, companies):
        company_list = list(companies)
        company_frequency = {}
        for company in company_list:
            freq = company_frequency.get(company, 0)
            freq += 1
            company_frequency[company] = freq

        num_complaints = len(list(company_list))
        num_companies = len(company_frequency)
        highest_percentage = round(100*max(map(lambda x: x[1], company_frequency.items()), default=0)/num_complaints)

        result = list(map(str, [productyear[0], productyear[1], num_complaints, num_companies, highest_percentage]))
        yield None, result
    
    def reducer3_sortformat_resultrows(self, _, resultrows):
        def quote_str_with_comma(s):
            if ',' in s:
                return '\"' + s + '\"'
            return s
        yield None, OUTPUT_HEADER
        for row in sorted(resultrows, key=lambda x: (x[0], x[1])):
            yield None, ",".join(map(quote_str_with_comma, row))
    
    def steps(self):
        return [
            MRStep(mapper_raw=self.mapper_raw1_csv_dictreader),
            MRStep(mapper=self.mapper2_productyear_complaint,reducer=self.reducer2_productyear_complaints),
            MRStep(reducer=self.reducer3_sortformat_resultrows)
        ]

if __name__ == "__main__":
    MRConsumerComplaints.run()