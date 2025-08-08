from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types

from flink_jobs.utils import parse_event


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    source = env.from_collection([
        '{"movie_id": 1, "rating": 8.7}',
        '{"movie_id": 1, "rating": 9.0}',
        '{"movie_id": 2, "rating": 7.5}',
    ])

    parsed = source.map(parse_event, output_type=Types.TUPLE([Types.INT(), Types.FLOAT()]))
    aggregated = parsed.key_by(lambda x: x[0]).reduce(lambda a, b: (a[0], a[1] + b[1]))
    aggregated.print()

    env.execute("ratings_stream")


if __name__ == "__main__":
    main()
