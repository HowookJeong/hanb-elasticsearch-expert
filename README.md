# hanb-elasticsearch-expert
- 실무 예제로 배우는 Elasticsearch - 활용편
- JUnit 기반의 TestCode 형식으로 작성되었습니다.
- 샘플 코드에 대한 데이터와 인덱스 맵핑 정보는 data, schema 경로에 위치해 있으니 참고 하시면 됩니다.

# Project 환경
- eclipse juno 또는 latest
- jdk 1.7 이상
- maven 3.0.4
- junit 4.8.2
- elasticsearch 1.3.x 이상
- elasticsearch 2.0.2
- hive 0.12.0
- hadoop 1.2.1

# Package 구성
## hanb.elasticsearch.expert.func
- AggregationTest
 - buckets, metrics 형태의 aggregation에 대한 사용법 및 샘플 코드를 제공 합니다.
- AutoCompletionTest
 - 자동완성 기능 구현에 사용되는 분석방법과 샘플 코드를 제공 합니다.
- EsHashPartitionTest
 - elasticsearch에서 document에 대한 shard allocation에 대한 샘플 코드를 제공 합니다.
- JoinNestedTest
 - nested document 구성을 통한 조인 기능 구현 방법과 샘플 코드를 제공 합니다.
- JoinParentChildTest
 - parent/child 구성을 통한 조인 기능 구현 방법과 샘플 코드를 제공 합니다.
- PercolateTest
 - percolate에 대한 활용 및 샘플 코드를 제공 합니다.
- RiverJdbcTest
 - JDBC river에 대한 활용 및 샘플 코드를 제공 합니다.

## hanb.elasticsearch.expert.hadoop.hive
- Indexer
 - hive를 이용한 색인 기능 샘플 코드를 제공 합니다.
- Searcher
 - hive를 이용한 질의 기능 샘플 코드를 제공 합니다.

## hanb.elasticsearch.expert.hadoop.mr
- IndexSearcher
 - mapreducer를 이용한 질의 기능 샘플 코드를 제공 합니다.
- IndexWriter
 - mapreducer를 이용한 색인 기능 샘플 코드를 제공 합니다.
