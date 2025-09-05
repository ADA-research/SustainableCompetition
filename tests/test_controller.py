import asyncio
import polars as pl
from sustainablecompetition.benchmarkingmethods.trivial_benchmarker import TrivialBenchmarker
from sustainablecompetition.controller import Controller
from sustainablecompetition.dataadapters.competition_dataadapter import CompetitionDataAdapter
from sustainablecompetition.infrastructureadapters.virtual_runner import VirtualRunner
from sustainablecompetition.resultconsumers.echo_consumer import EchoConsumer


def test_run():
    NJOBS = 50
    BENCHMARK_IDS = [str(i) for i in range(NJOBS)]
    NSOLVERS = 10
    df = pl.DataFrame({"hash": BENCHMARK_IDS, **{str(solver_id): [int(solver_id)] * NJOBS for solver_id in range(NSOLVERS)}})
    runner = VirtualRunner(CompetitionDataAdapter(df))
    benchmarks = df.select("hash").to_series().to_list()
    columns = df.columns
    method = TrivialBenchmarker(benchmarks, columns[1])
    consumer = EchoConsumer()
    controller = Controller(method, runner, njobs=1, consumers=[consumer])
    asyncio.run(controller.run())
