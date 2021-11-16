#!/usr/bin/env escript

-mode(compile).

-export([main/1]).

main([Minutes, FinalResultPath]) ->
    try
        MeasureAtMinute = list_to_integer(Minutes) / 2,
        if
            MeasureAtMinute < 1 ->
                MeasureAtSecond = trunc(MeasureAtMinute * 60),
                timer:sleep(timer:seconds(MeasureAtSecond));
            true ->
                timer:sleep(timer:minutes(trunc(MeasureAtMinute)))
        end,
        print_measurement(FinalResultPath)
    catch _:_ ->
        halt(1)
    end;

main(_) ->
    Name = filename:basename(escript:script_name()),
    io:fwrite(
        standard_error,
        "Usage: ~s bench_minutes path/to/write/to~n",
        [Name]
    ),
    halt(1).

print_measurement(Path) ->
    Now = calendar:system_time_to_rfc3339(erlang:system_time(millisecond), [{unit, millisecond}]),
    ok = file:write_file(Path, io_lib:format("~s~n", [Now])),
    [] = os:cmd(io_lib:format(
        "mpstat 5 1 | awk 'END{print 100-$NF}' >> ~s",
        [Path]
    )).
