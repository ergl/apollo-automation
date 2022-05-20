#!/usr/bin/env escript

-mode(compile).

-export([main/1]).

-type latencies() :: #{atom() => [tuple()]}.
-type latencies_graph() :: digraph:graph().

-define(LOCAL_SELF_DIR, "/Users/ryan/dev/imdea/code/automation").
-define(SELF_DIR, "/home/borja.deregil/automation").
-define(SSH_PRIV_KEY, "/home/borja.deregil/.ssh/id_ed25519").
-define(RESULTS_DIR, "/home/borja.deregil/results").
-define(VELETA_HOME, "/tmp/borja_experiments").

-define(IN_NODES_PATH,
    unicode:characters_to_list(io_lib:format("~s/execute-in-nodes.sh", [?SELF_DIR]))
).

-define(CONFIG_DIR,
    unicode:characters_to_list(io_lib:format("~s/configuration", [?SELF_DIR]))
).

-define(LOCAL_CONFIG_DIR,
    unicode:characters_to_list(io_lib:format("~s/configuration", [?LOCAL_SELF_DIR]))
).

-define(TOKEN_CONFIG,
    unicode:characters_to_list(io_lib:format("~s/secret.config", [?CONFIG_DIR]))
).

-define(CONF, configuration).

% 2 minute timeout for pmap
-define(TIMEOUT, timer:minutes(10)).
-define(RETRIES, 5).

-define(ALL_NODES,
    [
        %% Remove apollo-1-1 since its the master and experiment node
        'apollo-1-2.imdea',
        'apollo-1-3.imdea',
        'apollo-1-4.imdea',
        'apollo-1-5.imdea',
        'apollo-1-6.imdea',
        'apollo-1-7.imdea',
        'apollo-1-8.imdea',
        'apollo-1-9.imdea',
        'apollo-1-10.imdea',
        'apollo-1-11.imdea',
        'apollo-1-12.imdea',
        'apollo-2-1.imdea',
        'apollo-2-2.imdea',
        'apollo-2-3.imdea',
        'apollo-2-4.imdea',
        'apollo-2-5.imdea',
        'apollo-2-6.imdea',
        % 'apollo-2-7.imdea',
        'apollo-2-8.imdea',
        'apollo-2-9.imdea',
        'apollo-2-10.imdea',
        'apollo-2-11.imdea',
        'apollo-2-12.imdea'
    ]
).

-define(BIG_NODES,
    #{
        'veleta1' => [],
        'veleta2' => [],
        'veleta3' => [],
        'veleta4' => [],
        'veleta5' => [],
        'veleta6' => [],
        'veleta7' => [],
        'veleta8' => []
    }).

-type experiment_spec() :: #{config := string(), results_folder := string(), run_terms := [{atom(), term()}, ...]}.

usage() ->
    Name = filename:basename(escript:script_name()),
    ok = io:fwrite(
        standard_error,
        "Usage: ~s [-ds] --nodes <experiment-definition> --latencies <experiment-definition> | --generate <experiment-definition> | --experiment <experiment-definition>~n",
        [Name]
    ).

main(Args) ->
    case parse_args(Args) of
        {error, Reason} ->
            io:fwrite(standard_error, "Wrong option: reason ~s~n", [Reason]),
            usage(),
            halt(1);

        {ok, #{generate_definition := Definition}} ->
            {ok, DefinitionTerms} = file:consult(Definition),
            [#{
                config_terms := ConfigTerms,
                run_terms := RunTerms
            } | _] = materialize_experiments(?LOCAL_CONFIG_DIR, DefinitionTerms),
            ok = write_terms(filename:join(?LOCAL_CONFIG_DIR, "cluster_definition.config"), ConfigTerms),
            ok = write_terms(filename:join(?LOCAL_CONFIG_DIR, "run.config"), RunTerms),
            ok;

        {ok, #{show_latencies := Definition}} ->
            {ok, DefinitionTerms} = file:consult(Definition),
            Specs = materialize_experiments(?LOCAL_CONFIG_DIR, DefinitionTerms),
            io:format(
                "experiment, partitions, quorum_size, at_partition, at_region, commit_latency, delivery_latency\n"
            ),
            show_latencies(dedup_specs(Specs));

        {ok, #{show_nodes := Definition}} ->
            {ok, DefinitionTerms} = file:consult(Definition),
            Specs = materialize_experiments(?LOCAL_CONFIG_DIR, DefinitionTerms),
            Nodes = aggregate_nodes(dedup_specs(Specs)),
            io:format("Total Nodes: ~b~n~s~n", [sets:size(Nodes), format_node_set(Nodes)]);

        {ok, Opts = #{experiment_definition := Definition}} ->
            {ok, DefinitionTerms} = file:consult(Definition),
            Specs = materialize_experiments(?CONFIG_DIR, DefinitionTerms),
            run_experiments(Opts, Specs)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Parse, materialize experiments
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec materialize_experiments(string(), [{atom(), term()}, ...]) -> [experiment_spec()].
materialize_experiments(ConfigDir, Definition) ->
    {cluster_template, ClusterTemplate} = lists:keyfind(cluster_template, 1, Definition),
    {run_template, RunTemplate} = lists:keyfind(run_template, 1, Definition),
    {experiments, Experiments} = lists:keyfind(experiments, 1, Definition),
    {load_configuration, LoadSpec} = lists:keyfind(load_configuration, 1, Definition),
    {ok, ClusterTerms} = file:consult(filename:join([ConfigDir, ClusterTemplate])),
    {ok, RunTerms} = file:consult(filename:join([ConfigDir, RunTemplate])),
    %% Don't use flatmap, only flattens one level deep
    lists:flatten(
        lists:map(
            fun(Exp) -> materialize_single_experiment(ClusterTerms, RunTerms, LoadSpec, Exp) end,
            Experiments
        )
    ).

materialize_single_experiment(ClusterTerms, RunTerms, LoadSpec, Exp = #{clients := {M,F,A}}) ->
    [ materialize_single_experiment(ClusterTerms, RunTerms, LoadSpec, Exp#{clients => N}) || N <- apply(M, F, A) ];

materialize_single_experiment(ClusterTerms, RunTerms, LoadSpec, Exp = #{clients := List})
    when is_list(List) ->
        [ materialize_single_experiment(ClusterTerms, RunTerms, LoadSpec, Exp#{clients => N}) || N <- List ];

materialize_single_experiment(ClusterTerms, TemplateTerms, LoadSpec, Experiment = #{clients := N})
    when is_integer(N) ->
        ClientVariant =
            case lists:keyfind(driver, 1, TemplateTerms) of
                false ->
                    %% Go runner (new)
                    go_runner;
                {driver, _} ->
                    %% Erlang runner (old)
                    lasp_bench_runner
            end,

        %% Sanity check
        Workers = erlang:max(N, 1),

        % Set our number of threads
        TermsWithConcurrent = lists:keyreplace(concurrent, 1, TemplateTerms, {concurrent, Workers}),

        ReplaceKeyFun =
            fun(Key, Value, Acc) ->
                lists:keyreplace(Key, 1, Acc, {Key, Value})
            end,

        RunWith = maps:get(run_with, Experiment),

        % Verify that run terms is well formed

        VerifyOp =
            fun(Op, Keys) when is_list(Keys) ->
                lists:foreach(
                    fun(Key) ->
                        if
                            is_map_key(Key, RunWith) ->
                                ok;
                            true ->
                                io:fwrite(
                                    standard_error,
                                    "[~s] Operation ~p needed attribute ~p, but it's not present in spec~n",
                                    [maps:get(results_folder, Experiment), Op, Key]
                                ),
                                throw(error)
                        end
                    end,
                    Keys
                )
            end,

        VerifyOpMetrics =
            fun(Op, Keys) when is_list(Keys) ->
                case RunWith of
                    #{metrics := MetricList} when is_list(MetricList) ->
                        lists:foreach(
                            fun(Key) ->
                                case lists:member(Key, MetricList) of
                                    true ->
                                        ok;
                                    false ->
                                        io:fwrite(
                                            standard_error,
                                            "[~s] Operation ~p needed metric '~p', but it's not present in spec. Found metrics ~p~n",
                                            [maps:get(results_folder, Experiment), Op, Key, MetricList]
                                        ),
                                        throw(error)
                                end
                            end,
                            Keys
                        );
                    _ ->
                        io:fwrite(
                            standard_error,
                            "[~s] Operation ~p expected metrics ~p, but no metric is present~n",
                            [maps:get(results_folder, Experiment), Op, Keys]
                        ),
                        throw(error)
                end
            end,

        Ops = maps:get(operations, RunWith),
        lists:foreach(
            fun(Op) ->
                case Op of
                    read -> VerifyOp(Op, [readonly_ops]);
                    read_release -> VerifyOp(Op, [readonly_ops]);
                    read_random_pool -> VerifyOp(Op, [readonly_ops]);
                    read_distinct -> VerifyOp(Op, [readonly_ops]);
                    read_distinct_release -> VerifyOp(Op, [readonly_ops]);
                    read_distinct_measure -> VerifyOp(Op, [readonly_ops]);
                    read_distinct_no_commit -> VerifyOp(Op, [readonly_ops]);
                    read_track ->  VerifyOp(Op, [readonly_ops]);
                    update -> VerifyOp(Op, [writeonly_ops]);
                    update_distinct -> VerifyOp(Op, [writeonly_ops]);
                    update_distinct_measure -> VerifyOp(Op, [writeonly_ops]);
                    update_track -> VerifyOp(Op, [writeonly_ops]);
                    update_release -> VerifyOp(Op, [writeonly_ops]);
                    update_retry -> VerifyOp(Op, [writeonly_ops]);
                    update_release_retry -> VerifyOp(Op, [writeonly_ops]);
                    update_track_wait -> VerifyOp(Op, [writeonly_ops]);
                    update_measure -> VerifyOp(Op, [writeonly_ops]), VerifyOpMetrics(Op, [waitqueue_size, uncontended_keys_hit]);
                    mixed when ClientVariant =:= go_runner -> VerifyOp(Op, [readonly_ops, writeonly_ops]);
                    mixed when ClientVariant =:= lasp_bench_runner -> VerifyOp(Op, [mixed_read_write]);
                    no_tx_read -> VerifyOp(Op, [readonly_ops]);
                    no_tx_read_with_id -> VerifyOp(Op, [readonly_ops]);
                    _ ->
                        io:fwrite(
                            standard_error,
                            "[~s] Unknown operation specified: ~p~n",
                            [maps:get(results_folder, Experiment), Op]
                        ),
                        throw(error)
                end
            end,
            Ops
        ),

        ok = verify_partition_ranges(maps:get(results_folder, Experiment), RunWith),

        RunWith1 =
            case ClientVariant of
                go_runner ->
                    RunWith;
                lasp_bench_runner ->
                    RunWith0 =
                        case RunWith of
                            #{report_interval := {seconds, Secs}} ->
                                RunWith#{report_interval => Secs};
                            #{report_interval := Spec} ->
                                io:fwrite(
                                    standard_error,
                                    "[~s] Bad report interval spec: ~p~n",
                                    [maps:get(results_folder, Experiment), Spec]
                                ),
                                throw(error);
                            _ ->
                                RunWith
                        end,
                    RunWith0#{operations => [{OpName, 1} || OpName <- Ops]}
            end,

        % Fill all template values from experiment definition
        ExperimentTerms =
            maps:fold(ReplaceKeyFun, TermsWithConcurrent, RunWith1),

        % Fill all cluster template values from definition
        RunOnTerms = maps:get(run_on, Experiment),
        ConfigTerms =
            case RunOnTerms of
                #{clusters := ClusterMap} when is_map(ClusterMap) ->
                    %% Cluster literal, translate as is
                    maps:fold(ReplaceKeyFun, ClusterTerms, RunOnTerms);
                #{clusters := ClusterList} when is_list(ClusterList) ->
                    materialize_cluster_definition(RunOnTerms, ClusterTerms)
            end,

        ok = verify_leader_preferences(Experiment, maps:from_list(ConfigTerms)),

        Failures = maps:get(failures_after, Experiment, #{}),
        FailureSpec = build_failure_spec(Experiment, maps:from_list(ConfigTerms), Failures),

        case ClientVariant of
            lasp_bench_runner when is_map_key(crasher_after, Experiment) ->
                io:fwrite(
                    standard_error,
                    "[~s] Lasp bench runner doesn't support crasher specification~n",
                    [maps:get(results_folder, Experiment)]
                ),
                throw(error);
            _ ->
                ok
        end,

        Crasher = maps:get(crasher_after, Experiment, #{}),
        CrasherSpec = build_crasher_spec(
            Experiment,
            maps:from_list(ConfigTerms),
            maps:from_list(ExperimentTerms),
            LoadSpec,
            Crasher
        ),

        [
            #{
                config_terms => ConfigTerms,
                results_folder => maps:get(results_folder, Experiment),
                run_terms => ExperimentTerms,
                load_spec => LoadSpec,
                failure_spec => FailureSpec,
                crasher_spec => CrasherSpec
            }
        ].

%% TODO(borja): Verify that range is not larger than the number of partitions
verify_partition_ranges(ExperimentName, RunWith) ->
    UpperRange = maps:get(upper_partition_range, RunWith, -1),
    ok = verify_single_partition_range(ExperimentName, UpperRange),

    LowerRange = maps:get(lower_partition_range, RunWith, -1),
    ok = verify_single_partition_range(ExperimentName, LowerRange),

    if
        UpperRange < LowerRange ->
            io:fwrite(
                standard_error,
                "[~s] Upper partition range ~b is lower than lower range ~b~n",
                [ExperimentName, UpperRange, LowerRange]
            ),
            throw(error);
        true ->
            ok
    end.

verify_single_partition_range(ExperimentName, Range) when not is_integer(Range) ->
    io:fwrite(
        standard_error,
        "[~s] Upper partition range ~p is not a number~n",
        [ExperimentName, Range]
    ),
    throw(error);

verify_single_partition_range(ExperimentName, Range) when is_integer(Range) andalso Range < -1 ->
    io:fwrite(
        standard_error,
        "[~s] Upper partition range ~b is invalid~n",
        [ExperimentName, Range]
    ),
    throw(error);

verify_single_partition_range(_, _) ->
    ok.

build_failure_spec(Experiment, ConfigTerms, Failures) ->
    #{clusters := ClusterMap} = ConfigTerms,

    %% First, verify that all clusters specified in Failures do exist
    lists:foreach(
        fun
            ({C, _}) when is_map_key(C, ClusterMap) ->
                ok;
            (C) when is_map_key(C, ClusterMap) ->
                ok;
            (Unknown) ->
                io:fwrite(
                    standard_error,
                    "[~s] Failure map contains unrecognized replica: ~p~n",
                    [maps:get(results_folder, Experiment), Unknown]
                ),
                throw(error)
        end,
        maps:keys(Failures)
    ),

    maps:fold(
        fun(Group, TimeSpec, Acc) ->
            case parse_timeout_spec(TimeSpec) of
                error ->
                    io:fwrite(
                        standard_error,
                        "[~s] Failure map contains unrecognized time spec: ~p~n",
                        [maps:get(results_folder, Experiment), TimeSpec]
                    ),
                    throw(error);

                {ok, FailureTime} ->
                    Servers =
                        case Group of
                            {Cluster, Partition} ->
                                AllClusterServers = maps:get(servers, maps:get(Cluster, ClusterMap)),
                                [lists:nth(Partition + 1, lists:usort(AllClusterServers))];

                            Cluster ->
                                maps:get(servers, maps:get(Cluster, ClusterMap))
                        end,
                    lists:foldl(
                        fun(Server, InnerAcc) ->
                            InnerAcc#{Server => FailureTime}
                        end,
                        Acc,
                        Servers
                    )
            end
        end,
        #{},
        Failures
    ).

build_crasher_spec(_, _, _, _, M) when map_size(M) =:= 0 -> #{};
build_crasher_spec(Experiment, ConfigTerms, RunTerms, LoadSpec, #{
    replica := CrasherReplica,
    hot_key := HotKey,
    at := TimeSpec
}) ->

    %% First, verify that any cluster specified in Crasher does exist
    #{clusters := ClusterMap} = ConfigTerms,
    if
        not is_map_key(CrasherReplica, ClusterMap) ->
            io:fwrite(
                standard_error,
                "[~s] Crasher spec contains unrecognized replica: ~p~n",
                [maps:get(results_folder, Experiment), CrasherReplica]
            ),
            throw(error);
        true ->
            ok
    end,

    %% Verify that we have at least two partitions, as required by crasher
    maps:foreach(
        fun(Cluster, #{servers := Servers}) ->
            if
                length(Servers) < 2 ->
                    io:fwrite(
                        standard_error,
                        "[~s] Crasher spec needs at least two partitions, but replica ~p has ~b~n",
                        [maps:get(results_folder, Experiment), Cluster, length(Servers)]
                    ),
                    throw(error);

                true ->
                    ok
            end
        end,
        ClusterMap
    ),

    CrashAtTime =
        case parse_timeout_spec(TimeSpec) of
            error ->
                io:fwrite(
                    standard_error,
                    "[~s] Crasher spec contains unrecognized time spec: ~p~n",
                    [maps:get(results_folder, Experiment), TimeSpec]
                ),
                throw(error);

            {ok, FailureTime} ->
                FailureTime
        end,

    CrashKey = maps:get(magic_crash_key, ConfigTerms),
    if
        CrashKey =:= HotKey ->
            io:fwrite(
                standard_error,
                "[~s] Crasher spec: hot key and crash key are the same~n",
                [maps:get(results_folder, Experiment)]
            ),
            throw(error);

        true ->
            ok
    end,

    #{
        replica => CrasherReplica,

        master_node => maps:get(master_node, ConfigTerms),
        master_port => maps:get(master_port, ConfigTerms),

        magic_crash_key => CrashKey,
        hot_key => HotKey,

        op_timeout => maps:get(op_timeout, RunTerms),
        commit_timeout => maps:get(commit_timeout, RunTerms),

        value_bytes => maps:get(val_size, LoadSpec),

        crash_at => CrashAtTime
    }.

parse_timeout_spec(Time) when is_integer(Time) ->
    {ok, Time};

parse_timeout_spec({milliseconds, Time}) ->
    {ok, Time};

parse_timeout_spec({minutes, Time}) ->
    {ok, timer:minutes(Time)};

parse_timeout_spec({seconds, Time}) ->
    {ok, timer:seconds(Time)};

parse_timeout_spec(_) ->
    error.

verify_leader_preferences(Exp, #{leader_preference := Pref} = ConfigTerms) when is_atom(Pref) ->
    verify_leader_preferences(Exp, ConfigTerms#{leader_preference => [Pref]});

verify_leader_preferences(Experiment, #{leader_preference := PrefList} = ConfigTerms) when is_list(PrefList) ->
    #{clusters := ClusterMap} = ConfigTerms,
    if
        map_size(ClusterMap) =:= length(PrefList) ->
            %% Verify that all are known terms
            Check =
                lists:foldl(
                    fun
                        (_, {not_found, R}) -> {not_found, R};
                        (Replica, ok) when is_map_key(Replica, ClusterMap) -> ok;
                        (Replica, ok) -> {not_found, Replica}
                    end,
                    ok,
                    PrefList
                ),
            case Check of
                {not_found, R} ->
                    io:fwrite(
                        standard_error,
                        "[~s] Leader preference contains unrecognized replica: ~p~n",
                        [maps:get(results_folder, Experiment), R]
                    ),
                    throw(error);
                ok ->
                    ok
            end;

        true ->
            io:fwrite(
                standard_error,
                "[~s] Leader preference list does not match number of clusters: ~p~n",
                [maps:get(results_folder, Experiment), PrefList]
            ),
            throw(error)
    end;

verify_leader_preferences(Experiment, _) ->
    io:fwrite(
        standard_error,
        "[~s] Leader preference list is not present or badly formatted~n",
        [maps:get(results_folder, Experiment)]
    ),
    throw(error).

materialize_cluster_definition(RunOnTerms0, TemplateTerms) ->
    RunOnTerms =
        if
            is_map_key(leader_preference, RunOnTerms0) ->
                RunOnTerms0;
            true ->
                RunOnTerms0#{leader_preference => maps:get(clusters, RunOnTerms0)}
        end,

    SimpleReplacements = maps:without([leaders, clusters, partitions, per_partition, use_veleta], RunOnTerms),

    Replicas = maps:get(clusters, RunOnTerms),
    Clusters =
        build_cluster_map(
            Replicas,
            maps:get(partitions, RunOnTerms),
            maps:get(per_partition, RunOnTerms),
            maps:get(use_veleta, RunOnTerms, true)
        ),

    LeadersOnTemplate = maps:get(leaders, RunOnTerms, undefined),
    Leaders =
        build_leaders_map(
            maps:get(partitions, RunOnTerms),
            maps:keys(Clusters),
            LeadersOnTemplate
        ),

    maps:fold(
        fun(Key, Value, Acc) ->
            lists:keyreplace(Key, 1, Acc, {Key, Value})
        end,
        TemplateTerms,
        SimpleReplacements#{clusters => Clusters, leaders => Leaders}
    ).

build_cluster_map(Clusters, NPartitions, NClients, UseVeleta) ->
    PreferenceClients =
        if
            UseVeleta ->
                lists:sort(maps:keys(?BIG_NODES));
            true ->
                []
        end,
    RealClients =
        case NClients of
            auto ->
                AvailableForClients =
                    (length(?ALL_NODES) + length(PreferenceClients)) -
                        (NPartitions * length(Clusters)),
                AvailableForClients div length(Clusters);
            _ ->
                trunc(NPartitions * NClients)
        end,
    build_cluster_map(Clusters, NPartitions, RealClients, #{}, ?ALL_NODES, PreferenceClients).

build_cluster_map([], _, _, Acc, _, _) ->
    Acc;
build_cluster_map([ClusterName | Rest], NP, NClients, Acc, Available0, PreferenceClients0)
    when not is_map_key(ClusterName, Acc) ->
        {Servers, Available1} = lists:split(NP, Available0),
        TotalAvailable = length(PreferenceClients0) + length(Available1),
        if
            NClients > TotalAvailable ->
                io:fwrite(
                    standard_error,
                    "Not enough available machines for experiment: want ~b, have ~b~n",
                    [NClients, TotalAvailable]
                ),
                throw(error);
            true ->
                ok
        end,
        case length(PreferenceClients0) of
            N when N >= NClients ->
                {Clients, PreferenceClients1} = lists:split(NClients, PreferenceClients0),
                build_cluster_map(
                    Rest,
                    NP,
                    NClients,
                    Acc#{ClusterName => #{servers => Servers, clients => Clients}},
                    Available1,
                    PreferenceClients1
                );
            N when N > 0 ->
                {Clients0, PreferenceClients1} = lists:split(N, PreferenceClients0),
                {Clients1, Available2} = lists:split(NClients - N, Available1),
                build_cluster_map(
                    Rest,
                    NP,
                    NClients,
                    Acc#{ClusterName => #{servers => Servers, clients => Clients0 ++ Clients1}},
                    Available2,
                    PreferenceClients1
                );
            0 ->
                {Clients, Available2} = lists:split(NClients, Available1),
                build_cluster_map(
                    Rest,
                    NP,
                    NClients,
                    Acc#{ClusterName => #{servers => Servers, clients => Clients}},
                    Available2,
                    PreferenceClients0
                )
        end.

build_leaders_map(Partitions, ReplicaList, undefined) ->
    % If no leaders are specified, the leader for every partition is always the same
    Leader = hd(lists:sort(ReplicaList)),
    lists:foldl(
        fun(Partition, Acc) ->
            Acc#{(Partition-1) => Leader}
        end,
        #{},
        lists:seq(1, Partitions)
    );

build_leaders_map(Partitions, Replicas, Leaders) when is_map(Leaders) ->
    % Verify:
    LeaderSize = maps:size(Leaders),
    if
        Partitions =/= LeaderSize ->
            io:fwrite(
                standard_error,
                "Number of partitions (~b) and number of leaders (~b) doesn't match up!~n",
                [Partitions, LeaderSize]
            ),
            throw(error);
        true ->
            % Check that there's no replica in Leaders that doesn't exist
            ReplicaMap = lists:foldl(fun(R, M) -> M#{R => []} end, #{}, Replicas),
            AnyStray =
                lists:foldl(
                    fun
                        (_, {error, Stray}) ->
                            {error, Stray};
                        (R, ok) ->
                            if is_map_key(R, ReplicaMap) -> ok; true -> {error, R} end
                    end,
                    ok,
                    maps:values(Leaders)
                ),
            case AnyStray of
                {error, Stray} ->
                    io:fwrite(
                        standard_error,
                        "Unknown leader replica specified: ~p~n",
                        [Stray]
                    ),
                    throw(error);
                ok ->
                    Leaders
            end
    end;

build_leaders_map(Partitions, Replicas, Leader) when is_atom(Leader) ->
    case lists:member(Leader, Replicas) of
        true ->
            lists:foldl(
                fun(Partition, Acc) ->
                    Acc#{(Partition-1) => Leader}
                end,
                #{},
                lists:seq(1, Partitions)
            );
        false ->
            io:fwrite(
                standard_error,
                "Unknown leader replica specified: ~p~n",
                [Leader]
            ),
            throw(error)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Prepare experiment
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec cluster_config(experiment_spec()) -> [term()] | undefined.
cluster_config(#{config := Config}) -> Config;
cluster_config(_) -> undefined.

-spec get_next_cluster_config([experiment_spec()]) -> [term()] | undefined.
get_next_cluster_config([]) -> undefined;
get_next_cluster_config([Head | _]) -> cluster_config(Head).

-spec get_next_result_folder([experiment_spec()]) -> string() | undefined.
get_next_result_folder([ #{results_folder := Results} | _]) -> Results;
get_next_result_folder(_) -> undefined.

run_experiments(Opts, Specs) ->
    run_experiments(?RETRIES, Opts, undefined, Specs).

run_experiments(_, _, _, []) ->
    ok;

run_experiments(Retries, Opts, LastClusterTerms, [ Spec | Rest ]=AllSpecs) ->
    Result = execute_spec(
        Opts,
        LastClusterTerms,
        Spec,
        get_next_cluster_config(Rest),
        get_next_result_folder(Rest)
    ),
    case Result of
        ok ->
            %% Start next spec with fresh retries
            run_experiments(?RETRIES, Opts, cluster_config(Spec), Rest);

        {error, Reason} when Retries > 0 ->
            SpecTitle = maps:get(results_folder, Spec),
            io:fwrite(
                standard_error,
                "Retrying spec ~s error (~b/~b)~nReason: ~p~nFull spec: ~p~n",
                [SpecTitle, Retries, ?RETRIES, Reason, Spec]
            ),
            %% Retry again, with last cluster as undefined so that we can start from a clean slate
            run_experiments(Retries - 1, Opts, undefined, AllSpecs);

        {error, Reason} ->
            SpecTitle = maps:get(results_folder, Spec),
            io:fwrite(
                standard_error,
                "Spec error on ~s~nReason:~p~nFull spec: ~p~n",
                [SpecTitle, Reason, Spec]
            ),
            error;

        {fatal_error, Reason} ->
            SpecTitle = maps:get(results_folder, Spec),
            io:fwrite(
                standard_error,
                "Fatal spec error on ~s~nReason: ~p~nFull spec: ~p~n",
                [SpecTitle, Reason, Spec]),
            error
    end.

execute_spec(Opts, PrevConfigTerms, Spec, NextConfigTerms, NextResults) ->
    #{
        config_terms := ConfigTerms,
        results_folder := Results,
        run_terms := RunTerms,
        load_spec := LoadSpec,
        failure_spec := FailureSpec,
        crasher_spec := CrasherSpec
    } = Spec,

    _ = ets:new(?CONF, [set, named_table]),
    case catch preprocess_args(Opts, ConfigTerms, RunTerms) of
        {'EXIT', TraceBack} ->
            ets:delete(?CONF),
            {fatal_error, TraceBack};

        {ClusterMap, Master} ->
            ConfigFile = "cluster_definition.config",
            Result =
                try
                    case ConfigTerms =:= PrevConfigTerms of
                        true ->
                            %% We're reusing the same cluster, no need to download anything.
                            %% Just check if something went wrong.
                            ok = check_nodes(Master, ClusterMap);
                        false ->
                            %% This is a new cluster, past spec cleaned up, so we need to re-download things
                            ok = write_terms(filename:join(?CONFIG_DIR, ConfigFile), ConfigTerms),
                            ok = check_nodes(Master, ClusterMap),
                            ok = push_scripts(ConfigFile, Master, ClusterMap),

                            ok = download_master(Master),
                            ok = download_server(ConfigFile, ClusterMap),
                            ok = download_runner(ClusterMap)
                    end,

                    %% Start things, re-sync NTP
                    ok = sync_nodes(Master, ClusterMap),
                    ok = start_master(Master, ConfigTerms),
                    ok = start_server(ConfigFile, ClusterMap),

                    % Wait a bit for all servers to connect to each other before
                    % we start the benchmark. Otherwise, we might get ECONNREFUSED
                    % since servers don't start their client servers until they're
                    % ready
                    ok = timer:sleep(1000),

                    %% Actual experiment: load then bench
                    ok = load_ext(Master, ClusterMap, LoadSpec),
                    ok = bench_ext(Master, RunTerms, ClusterMap, ConfigFile, FailureSpec, CrasherSpec),

                    %% Give system some time (1 sec) to stabilise
                    ok = timer:sleep(1000),

                    %% If we enabled CPU profiling, make sure to dump it to disk
                    ok = dump_server_profile(ConfigFile, ClusterMap),

                    %% Gather all results from the experiment
                    %% If Results =/= NextResults, then we can archive the entire path
                    ShouldArchive =
                        case Results of
                            NextResults -> false;
                            _ -> {archive, Results}
                        end,
                    ok = pull_results(
                        ConfigTerms,
                        ConfigFile,
                        Results,
                        RunTerms,
                        ClusterMap,
                        ShouldArchive
                    ),

                    %% Stop all nodes
                    ok = stop_master(Master),
                    ok = stop_server(ConfigFile, ClusterMap),

                    case ConfigTerms =:= NextConfigTerms of
                        true ->
                            %% Next experiment will reuse our cluster, no need to clean up
                            ok;
                        false ->
                            %% Clean up after the experiment
                            ok = cleanup_master(Master),
                            ok = cleanup_servers(ClusterMap),
                            ok = cleanup_clients(ClusterMap)
                    end,

                    ok
                catch
                    error:Exception:Stack ->
                        %% An exception happened, clean up everything just in case
                        brutal_client_kill(ClusterMap),
                        cleanup_master(Master),
                        cleanup_servers(ClusterMap),
                        cleanup_clients(ClusterMap),
                        {error, {Exception, Stack}};

                    throw:Term:Stack ->
                        %% An exception happened, clean up everything just in case
                        brutal_client_kill(ClusterMap),
                        cleanup_master(Master),
                        cleanup_servers(ClusterMap),
                        cleanup_clients(ClusterMap),
                        {error, {Term, Stack}}
                end,
            ets:delete(?CONF),
            Result
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Set up experiment
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

preprocess_args(Opts, ConfigTerms, RunTerms) ->
    {ok, TokenTerms} = file:consult(?TOKEN_CONFIG),
    {token, Token} = lists:keyfind(token, 1, TokenTerms),
    true = ets:insert(?CONF, {token, Token}),

    {clusters, ClusterMap} = lists:keyfind(clusters, 1, ConfigTerms),

    {ext_tag, ExtTag} = lists:keyfind(ext_tag, 1, ConfigTerms),
    true = ets:insert(?CONF, {ext_tag, ExtTag}),

    {ext_local_port, LocalPort} = lists:keyfind(ext_local_port, 1, ConfigTerms),
    true = ets:insert(?CONF, {ext_local_port, LocalPort}),

    {leaders, Leaders} = lists:keyfind(leaders, 1, ConfigTerms),
    true = ets:insert(?CONF, {leaders, Leaders}),

    case lists:keyfind(cpu_profile, 1, ConfigTerms) of
        {cpu_profile, ProfilePath} when is_list(ProfilePath) ->
            true = ets:insert(?CONF, {cpu_profile, ProfilePath});
        _ ->
            % Key might not be present, or might be default ('_')
            ok
    end,

    Servers = ordsets:from_list(server_nodes(ClusterMap)),
    Clients = ordsets:from_list(client_nodes(ClusterMap)),
    case ordsets:is_disjoint(Servers, Clients) of
        false ->
            io:fwrite(
                standard_error,
                "Bad cluster map: clients and servers overlap~n",
                []
            ),
            erlang:throw(cluster_overlap);
        true ->
            ok
    end,

    BigNodes = ordsets:from_list(maps:keys(?BIG_NODES)),
    case ordsets:is_disjoint(Servers, BigNodes) of
        false ->
            io:fwrite(
                standard_error,
                "Bad cluster map: can't use veleta machines for servers~n",
                []
            ),
            erlang:throw(veleta_servers);
        true ->
            ok
    end,

    {master_node, Master} = lists:keyfind(master_node, 1, ConfigTerms),
    {master_port, MasterPort} = lists:keyfind(master_port, 1, ConfigTerms),
    case ordsets:is_element(Master, ordsets:union(Servers, Clients)) of
        true ->
            io:fwrite(
                standard_error,
                "Bad master: master is also a server or client~n",
                []
            ),
            erlang:throw(master_overlap);
        false ->
            ok
    end,
    true = ets:insert(?CONF, {master_node, Master}),
    true = ets:insert(?CONF, {master_port, MasterPort}),

    AllPartitions =
        [ length(S) || #{servers := S} <- maps:values(ClusterMap) ],

    case ordsets:size(ordsets:from_list(AllPartitions)) =:= 1 of
        false ->
            io:fwrite(
                standard_error,
                "Bad clustermap: different number of servers across replicas~n",
                []
            ),
            erlang:throw(partition_mismatch);
        true ->
            ok
    end,
    true = ets:insert(?CONF, {n_replicas, maps:size(ClusterMap)}),
    true = ets:insert(?CONF, {n_partitions, hd(AllPartitions)}),

    true = ets:insert(?CONF, {dry_run, maps:get(dry_run, Opts, false)}),
    true = ets:insert(?CONF, {silent, maps:get(verbose, Opts, false)}),

    ClientVariant =
        case lists:keyfind(driver, 1, RunTerms) of
            false ->
                %% Go runner (new)
                go_runner;
            {driver, _} ->
                %% Erlang runner (old)
                lasp_bench_runner
        end,
    true = ets:insert(?CONF, {client_variant, ClientVariant}),

    {ClusterMap, Master}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Experiment Steps
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec check_nodes(_, _) -> ok | {error, term()}.
check_nodes(Master, ClusterMap) ->
    io:format("Checking that all nodes are up and on the correct governor mode~n"),

    AllNodes = [Master | all_nodes(ClusterMap)],

    UptimeRes = do_in_nodes_par("uptime", AllNodes, ?TIMEOUT),
    ok = lists:foldl(
        fun
            (_, {error, Node}) ->
                {error, Node};

            ({Node, Res}, ok) ->
                case string:str(Res, "timed out") of
                    0 ->
                        ok;
                    _ ->
                        {error, Node}
                end
        end,
        ok,
        lists:zip(AllNodes, UptimeRes)
    ),

    VeletaNodes =
        lists:filter(fun(N) -> is_map_key(N, ?BIG_NODES) end, AllNodes),

    SetUpHomeFolder =
        do_in_nodes_par(io_lib:format("mkdir -p ~s", [?VELETA_HOME]), VeletaNodes, ?TIMEOUT),

    ok = lists:foldl(
        fun
            (_, {error, Node}) ->
                {error, Node};
            ({Node, Res}, ok) ->
                case Res of
                    "" ->
                        ok;
                    _ ->
                        {error, Node}
                end
        end,
        ok,
        lists:zip(VeletaNodes, SetUpHomeFolder)
    ),

    % Veleta nodes don't have scaling_governor available, skip them
    NoVeletaNodes =
        lists:filter(
            fun
                (N) when is_map_key(N, ?BIG_NODES) -> false;
                (_) -> true
            end,
            AllNodes
        ),

    % Set all nodes to performance governor status, then verify
    _ = do_in_nodes_par(
        "echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor",
        NoVeletaNodes,
        ?TIMEOUT
    ),
    GovernorStatus = do_in_nodes_par(
        "cat /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor",
        NoVeletaNodes,
        ?TIMEOUT
    ),
    ok = lists:foldl(
        fun
            (_, {error, Node}) ->
                {error, Node};

            ({Node, Res}, ok) ->
                case string:str(Res, "powersave") of
                    0 ->
                        ok;
                    _ ->
                        {error, Node}
                end
        end,
        ok,
        lists:zip(NoVeletaNodes, GovernorStatus)
    ),
    ok.

-spec push_scripts(_, _, _) -> _ | {error, timeout}.
push_scripts(ConfigFile, Master, ClusterMap) ->
    % Transfer server, bench and cluster config
    AllNodes = [Master | all_nodes(ClusterMap)],
    io:format("Transfering benchmark config files (server, bench, cluster)...~n"),
    case
        pmap(
            fun(Node) ->
                transfer_script(Node, "master.sh"),
                transfer_script(Node, "server.escript"),
                transfer_script(Node, "bench.sh"),
                transfer_script(Node, "erlang_bench.sh"),
                transfer_script(Node, "my_ip"),
                transfer_script(Node, "fetch_gh_release.sh"),
                transfer_script(Node, "measure_cpu.sh"),
                transfer_config(Node, ConfigFile)
            end,
            AllNodes,
            ?TIMEOUT
        )
    of
        {error, Timeout} ->
            {error, Timeout};
        _ ->
            ok
    end.

sync_nodes(Master, ClusterMap) ->
    io:format("Resyncing NTP on all nodes~n"),
    AllNodes = [Master | all_nodes(ClusterMap)],
    case do_in_nodes_par("sudo service ntp stop", AllNodes, ?TIMEOUT) of
        {error, _} -> error;
        _ ->
            case do_in_nodes_par("sudo ntpd -gq system.imdea", AllNodes, ?TIMEOUT) of
                {error, _} -> error;
                _ ->
                    case do_in_nodes_par("sudo service ntp start", AllNodes, ?TIMEOUT) of
                        {error, _} -> error;
                        _ -> ok
                    end
            end
    end.

download_master(Master) ->
    AuthToken = ets:lookup_element(?CONF, token, 2),
    GitTag = ets:lookup_element(?CONF, ext_tag, 2),
    case do_in_nodes_par(master_command(GitTag, "download", AuthToken), [Master], ?TIMEOUT) of
        {error, Reason} ->
            {error, Reason};
        Print ->
            io:format("~p~n", [Print]),
            ok
    end.

start_master(Master, ConfigTerms) ->
    GitTag = ets:lookup_element(?CONF, ext_tag, 2),
    NumReplicas = ets:lookup_element(?CONF, n_replicas, 2),
    NumPartitions = ets:lookup_element(?CONF, n_partitions, 2),
    {_, Latencies} = lists:keyfind(latencies, 1, ConfigTerms),
    {_, Preferences} = lists:keyfind(leader_preference, 1, ConfigTerms),

    % Build the arguments for master
    ArgString0 =
        maps:fold(
            fun(Partition, Replica, Acc) ->
                io_lib:format(
                    "-leader ~b:~s ~s",
                    [Partition, atom_to_list(Replica), Acc]
                )
            end,
            "",
            ets:lookup_element(?CONF, leaders, 2)
        ),

    ArgString1 =
        lists:foldl(
            fun(Replica, Acc) ->
                io_lib:format("~s -leaderChoice ~s", [Acc, Replica])
            end,
            ArgString0,
            Preferences
        ),

    ArgString2 =
        maps:fold(
            fun(FromReplica, ToReplicas, Acc) ->
                lists:foldl(
                    fun({ToReplica, Latency}, InnerAcc) ->
                        io_lib:format(
                            "~s -replicaLatency ~s:~s:~b",
                            [InnerAcc, FromReplica, ToReplica, Latency]
                        )
                    end,
                    Acc,
                    ToReplicas
                )
            end,
            ArgString1,
            Latencies
        ),

    case
        do_in_nodes_par(
                master_command(
                    GitTag,
                    "run",
                    integer_to_list(NumReplicas),
                    integer_to_list(NumPartitions),
                    ArgString2
                ),
                [Master],
                ?TIMEOUT
            )
    of
        {error, _} ->
            error;
        Print ->
            io:format("~p~n", [Print]),
            ok
    end.

stop_master(Master) ->
    GitTag = ets:lookup_element(?CONF, ext_tag, 2),
    case do_in_nodes_par(master_command(GitTag, "stop"), [Master], ?TIMEOUT) of
        {error, _} ->
            error;
        _ ->
            ok
    end.

download_server(ConfigFile, ClusterMap) ->
    AuthToken = ets:lookup_element(?CONF, token, 2),
    case do_in_nodes_par(server_command(ConfigFile, "download", AuthToken), server_nodes(ClusterMap), ?TIMEOUT) of
        {error, Reason} ->
            {error, Reason};
        Print ->
            io:format("~p~n", [Print]),
            ok
    end.

start_server(ConfigFile, ClusterMap) ->
    remove_server_logs(ClusterMap),
    maps:fold(
        fun
            (_, _, error) ->
                error;
            (ClusterName, _, ok) ->
                {SortedNames, Index} = server_nodes_with_partitions(ClusterName, ClusterMap),
                case
                    do_in_nodes_par_func(
                        fun(NodeStr) ->
                            Partition = maps:get(list_to_atom(NodeStr), Index),
                            server_command(ConfigFile, "start", atom_to_list(ClusterName), Partition)
                        end,
                        SortedNames,
                        ?TIMEOUT
                    )
                of
                    {error, _} ->
                        error;
                    Res ->
                        io:format("~p: ~p~n", [ClusterName, Res]),
                        ok
                end
        end,
        ok,
        ClusterMap
    ).

remove_server_logs(ClusterMap) ->
    pmap(
        fun(Node) ->
            NodeStr = atom_to_list(Node),
            LogPath = io_lib:format("~s/screenlog.0", [home_path_for_node(NodeStr)]),
            Command = io_lib:format("rm -rf ~s", [LogPath]),
            Cmd = io_lib:format("~s \"~s\" ~s", [?IN_NODES_PATH, Command, NodeStr]),
            safe_cmd(Cmd)
        end,
        server_nodes(ClusterMap),
        ?TIMEOUT
    ).

stop_server(ConfigFile, ClusterMap) ->
    case do_in_nodes_par(server_command(ConfigFile, "stop"), server_nodes(ClusterMap), ?TIMEOUT) of
        {error, _} ->
            error;
        Res ->
            io:format("~p~n", [Res]),
            ok
    end.

stop_single_server(ConfigFile, Node) ->
    case do_in_nodes_par(server_command(ConfigFile, "stop"), [Node], ?TIMEOUT) of
        {error, _} ->
            error;
        Res ->
            io:format("~p~n", [Res]),
            ok
    end.

dump_server_profile(ConfigFile, ClusterMap) ->
    case ets:lookup(?CONF, cpu_profile) of
        [] ->
            ok;
        _ ->
            case do_in_nodes_par(server_command(ConfigFile, "profile"), server_nodes(ClusterMap), ?TIMEOUT) of
                {error, _} ->
                    error;
                Res ->
                    io:format("~p~n", [Res]),
                    ok
            end
    end.

brutal_client_kill(ClusterMap) ->
    brutal_client_kill(ets:lookup_element(?CONF, client_variant, 2), ClusterMap).

brutal_client_kill(go_runner, ClusterMap) ->
    _ = do_in_nodes_par("kill -9 \\$(pgrep -f runner_linux_amd64)", client_nodes(ClusterMap), ?TIMEOUT),
    ok;

brutal_client_kill(lasp_bench_runner, ClusterMap) ->
    _ = do_in_nodes_par("pkill -9 beam", client_nodes(ClusterMap), ?TIMEOUT),
    ok.

download_runner(ClusterMap) ->
    download_runner(ets:lookup_element(?CONF, client_variant, 2), ClusterMap).

download_runner(go_runner, ClusterMap) ->
    AuthToken = ets:lookup_element(?CONF, token, 2),
    GitTag = ets:lookup_element(?CONF, ext_tag, 2),
    NodeNames = client_nodes(ClusterMap),

    DownloadRes = do_in_nodes_par_func(
        fun(Node) -> client_command(Node, GitTag, "download", AuthToken) end,
        NodeNames,
        ?TIMEOUT
    ),

    case DownloadRes of
        {error, Reason} ->
            {error, Reason};
        _ ->
            ok
    end;

download_runner(lasp_bench_runner, ClusterMap) ->
    NodeNames = client_nodes(ClusterMap),

    DownloadRes = do_in_nodes_par_func(
        fun(Node) -> erlang_client_command(Node, "download") end,
        NodeNames,
        ?TIMEOUT
    ),

    case DownloadRes of
        {error, Reason} ->
            {error, Reason};
        _ ->
            CompileRes = do_in_nodes_par_func(
                fun(Node) -> erlang_client_command(Node, "compile") end,
                NodeNames,
                ?TIMEOUT
            ),
            case CompileRes of
                {error, Reason1} ->
                    {error, Reason1};
                _ ->
                    ok
            end
    end.

load_ext(Master, ClusterMap, LoadSpec) ->
    load_ext(ets:lookup_element(?CONF, client_variant, 2), Master, ClusterMap, LoadSpec).

load_ext(go_runner, Master, ClusterMap, LoadSpec) ->
    Keys = maps:get(key_limit, LoadSpec, 1_000_000),
    ValueBytes = maps:get(val_size, LoadSpec, 256),

    GitTag = ets:lookup_element(?CONF, ext_tag, 2),
    MasterPort = ets:lookup_element(?CONF, master_port, 2),

    TargetClients =
        maps:fold(
            fun(Replica, #{clients := C}, Acc) ->
                [ { Replica, hd(lists:usort(C)) } | Acc ]
            end,
            [],
            ClusterMap
        ),

    Res =
        pmap(
            fun({TargetReplica, ClientNode}) ->
                ClientNodeStr = atom_to_list(ClientNode),
                Command = client_command(
                    ClientNodeStr,
                    GitTag,
                    "load_ext",
                    atom_to_list(Master),
                    integer_to_list(MasterPort),
                    atom_to_list(TargetReplica),
                    integer_to_list(Keys),
                    integer_to_list(ValueBytes)
                ),
                Cmd = io_lib:format(
                    "~s \"~s\" ~s",
                    [?IN_NODES_PATH, Command, ClientNodeStr]
                ),
                safe_cmd(Cmd)
            end,
            TargetClients,
            ?TIMEOUT
        ),

    case Res of
        {error, _} ->
            error;
        _ ->
            ok
    end;

load_ext(lasp_bench_runner, Master, ClusterMap, LoadSpec) ->
    Keys = maps:get(key_limit, LoadSpec, 1_000_000),
    ValueBytes = maps:get(val_size, LoadSpec, 256),

    MasterPort = ets:lookup_element(?CONF, master_port, 2),

    TargetClients =
        maps:fold(
            fun(Replica, #{clients := C}, Acc) ->
                [ { Replica, hd(lists:usort(C)) } | Acc ]
            end,
            [],
            ClusterMap
        ),

    Res =
        pmap(
            fun({TargetReplica, ClientNode}) ->
                ClientNodeStr = atom_to_list(ClientNode),
                Command = erlang_client_command(
                    ClientNodeStr,
                    "load_ext",
                    atom_to_list(Master),
                    integer_to_list(MasterPort),
                    atom_to_list(TargetReplica),
                    integer_to_list(Keys),
                    integer_to_list(ValueBytes)
                ),
                Cmd = io_lib:format(
                    "~s \"~s\" ~s",
                    [?IN_NODES_PATH, Command, ClientNodeStr]
                ),
                safe_cmd(Cmd)
            end,
            TargetClients,
            ?TIMEOUT
        ),

    case Res of
        {error, _} ->
            error;
        _ ->
            ok
    end.

bench_ext(Master, RunTerms, ClusterMap, ConfigFile, FailureSpec, CrasherSpec) ->
    bench_ext(ets:lookup_element(?CONF, client_variant, 2), Master, RunTerms, ClusterMap, ConfigFile, FailureSpec, CrasherSpec).

bench_ext(go_runner, Master, RunTerms, ClusterMap, ConfigFile, FailureSpec, CrasherSpec) ->
    GitTag = ets:lookup_element(?CONF, ext_tag, 2),

    NodesWithReplicas = [
        {Replica, N} ||
            {Replica, #{clients := C}} <- maps:to_list(ClusterMap),
            N <- lists:usort(C)
    ],

    ArgumentString =
        lists:foldl(
            fun(Elt, Acc) ->
                case Elt of
                    {duration, Minutes} ->
                        io_lib:format("~s -duration ~bm", [Acc, Minutes]);
                    {report_interval, ReportTimeSpec} ->
                        {ok, Millis} = parse_timeout_spec(ReportTimeSpec),
                        io_lib:format("~s -reportInterval ~s", [Acc, to_go_duration(Millis)]);
                    {concurrent, Threads} ->
                        io_lib:format("~s -concurrent ~b", [Acc, Threads]);
                    {key_range, Keys} ->
                        io_lib:format("~s -keyRange ~b", [Acc, Keys]);
                    {value_bytes, Bytes} ->
                        io_lib:format("~s -valueBytes ~b", [Acc, Bytes]);
                    {conn_pool_size, PoolSize} ->
                        io_lib:format("~s -poolSize ~b", [Acc, PoolSize]);
                    {readonly_ops, N} when is_integer(N) ->
                        io_lib:format("~s -readKeys ~b", [Acc, N]);
                    {writeonly_ops, N} when is_integer(N) ->
                        io_lib:format("~s -writeKeys ~b", [Acc, N]);
                    {retry_aborts, true} ->
                        io_lib:format("~s -retryAbort", [Acc]);
                    {key_distribution, uniform} ->
                        io_lib:format("~s -distribution uniform", [Acc]);
                    {key_distribution, pareto} ->
                        io_lib:format("~s -distribution pareto", [Acc]);
                    {key_distribution, split_uniform} ->
                        io_lib:format("~s -distribution split_uniform", [Acc]);
                    {key_distribution, {biased_key, Key, Bias}} when is_integer(Key) andalso is_integer(Bias) ->
                        io_lib:format("~s -distribution biased_key -distrArgs '-hotKey ~b -bias ~b'", [Acc, Key, Bias]);
                    {key_distribution, {biased_key_worker_id, Key, Bias}} when is_integer(Key) andalso is_integer(Bias) ->
                        io_lib:format("~s -distribution biased_key_by_worker -distrArgs '-hotKey ~b -bias ~b'", [Acc, Key, Bias]);
                    {key_distribution, {constant_key, Key}} when is_integer(Key) ->
                        io_lib:format("~s -distribution constant_key -distrArgs '-key ~b'", [Acc, Key]);
                    {operations, OpList} ->
                        lists:foldl(
                            fun(Op, InnerAcc) ->
                                io_lib:format("~s -operation ~s", [InnerAcc, atom_to_list(Op)])
                            end,
                            Acc,
                            OpList
                        );
                    {metrics, MetricList} when is_list(MetricList) ->
                        lists:foldl(
                            fun(Metric, InnerAcc) ->
                                io_lib:format("~s -metric ~s", [InnerAcc, atom_to_list(Metric)])
                            end,
                            Acc,
                            MetricList
                        );
                    {op_timeout, TimeoutSpec} ->
                        {ok, Millis} = parse_timeout_spec(TimeoutSpec),
                        io_lib:format("~s -opTimeout ~s", [Acc, to_go_duration(Millis)]);
                    {commit_timeout, TimeoutSpec} ->
                        {ok, Millis} = parse_timeout_spec(TimeoutSpec),
                        io_lib:format("~s -commitTimeout ~s", [Acc, to_go_duration(Millis)]);
                    {upper_partition_range, UpperPartitionRange} when is_integer(UpperPartitionRange) ->
                        io_lib:format("~s -upperPartitionRange ~b", [Acc, UpperPartitionRange]);
                    {lower_partition_range, LowerPartitionRange} when is_integer(LowerPartitionRange) ->
                        io_lib:format("~s -lowerPartitionRange ~b", [Acc, LowerPartitionRange]);
                    _ ->
                        Acc
                end
            end,
            "",
            RunTerms
        ),

    MasterPort = ets:lookup_element(?CONF, master_port, 2),

    %% Set up measurements. Sleep on instrumentation node for a bit, then send the script and collect metrics
    RunsForMinutes = proplists:get_value(duration, RunTerms),
    MeasureAtMinute = RunsForMinutes / 2,
    MeasureAt =
        if MeasureAtMinute < 1 ->
            timer:seconds(trunc(MeasureAtMinute * 60));
        true ->
            timer:minutes(trunc(MeasureAtMinute))
        end,

    % Spawn the CPU measurements
    Token = async_for(
        fun(Node) ->
            NodeStr = atom_to_list(Node),
            HomePath = home_path_for_node(NodeStr),
            CPUPath = filename:join(
                home_path_for_node(NodeStr),
                io_lib:format("~s.cpu", [NodeStr])
            ),
            Cmd0 = io_lib:format(
                "~s/measure_cpu.sh -f ~s",
                [HomePath, CPUPath]
            ),
            timer:sleep(MeasureAt),
            Cmd = io_lib:format("~s \"~s\" ~s", [?IN_NODES_PATH, Cmd0, NodeStr]),
            safe_cmd(Cmd)
        end,
        all_nodes(ClusterMap)
    ),

    %% Cleanup result path first
    pmap(
        fun({_, Node}) ->
            NodeStr = atom_to_list(Node),
            ResultPath = io_lib:format("~s/runner_results/current", [home_path_for_node(NodeStr)]),
            Command = io_lib:format("rm -rf ~s", [ResultPath]),
            Cmd = io_lib:format("~s \"~s\" ~s", [?IN_NODES_PATH, Command, NodeStr]),
            safe_cmd(Cmd)
        end,
        NodesWithReplicas,
        ?TIMEOUT
    ),

    % If any, kill some server nodes after the specified time
    maps:foreach(
        fun(Server, FailAfter) ->
            erlang:spawn(
                fun() ->
                    ok = timer:sleep(FailAfter),
                    stop_single_server(ConfigFile, Server)
                end
            )
        end,
        FailureSpec
    ),

    case CrasherSpec of
        #{crash_at := CrashAfter} ->
            [{_Replica, HeadNode} | _]=  NodesWithReplicas,
            erlang:spawn(
                fun() ->
                    ok = timer:sleep(CrashAfter),
                    spawn_crasher(GitTag, HeadNode, CrasherSpec)
                end
            );
        _ ->
            ok
    end,

    %% Wait at least the same time that the benchmark is supposed to run
    BenchTimeout = timer:minutes(RunsForMinutes) + ?TIMEOUT,
    pmap(
        fun({Replica, Node}) ->
            NodeStr = atom_to_list(Node),
            ResultPath = io_lib:format("~s/runner_results/current", [home_path_for_node(NodeStr)]),
            NodeArgList = io_lib:format(
                "-replica ~s -master_ip ~s -master_port ~b -resultPath ~s ~s",
                [atom_to_list(Replica), atom_to_list(Master), MasterPort, ResultPath, ArgumentString]
            ),

            Command = client_command(
                NodeStr,
                GitTag,
                "run",
                NodeArgList
            ),

            Cmd = io_lib:format("~s \"~s\" ~s", [?IN_NODES_PATH, Command, NodeStr]),
            safe_cmd(Cmd)
        end,
        NodesWithReplicas,
        BenchTimeout
    ),

    %% Ensure that measurements have terminated
    %% TODO(borja): Since measure_cpu takes half the benchmark, this timeout should be tweaked.
    case async_for_receive(Token, ?TIMEOUT) of
        {error, timeout} ->
            {error, timeout};
        _ ->
            ok
    end;

bench_ext(lasp_bench_runner, Master, RunTerms, ClusterMap, ConfigFile, FailureSpec, _) ->
    ok = write_terms(filename:join(?CONFIG_DIR, "run.config"), RunTerms),
    Res = pmap(fun(Node) -> transfer_config(Node, "run.config") end, client_nodes(ClusterMap), ?TIMEOUT),
    case Res of
        {error, _} ->
            error;
        _ ->
            NodesWithReplicas = [
                {Replica, N} ||
                    {Replica, #{clients := C}} <- maps:to_list(ClusterMap),
                    N <- lists:usort(C)
            ],

            MasterPort = ets:lookup_element(?CONF, master_port, 2),

            %% Set up measurements. Sleep on instrumentation node for a bit, then send the script and collect metrics
            RunsForMinutes = proplists:get_value(duration, RunTerms),
            MeasureAtMinute = RunsForMinutes / 2,
            MeasureAt =
                if MeasureAtMinute < 1 ->
                    timer:seconds(trunc(MeasureAtMinute * 60));
                true ->
                    timer:minutes(trunc(MeasureAtMinute))
                end,

            % Spawn the CPU measurements
            Token = async_for(
                fun(Node) ->
                    NodeStr = atom_to_list(Node),
                    HomePath = home_path_for_node(NodeStr),
                    CPUPath = filename:join(
                        home_path_for_node(NodeStr),
                        io_lib:format("~s.cpu", [NodeStr])
                    ),
                    Cmd0 = io_lib:format(
                        "~s/measure_cpu.sh -f ~s",
                        [HomePath, CPUPath]
                    ),
                    timer:sleep(MeasureAt),
                    Cmd = io_lib:format("~s \"~s\" ~s", [?IN_NODES_PATH, Cmd0, NodeStr]),
                    safe_cmd(Cmd)
                end,
                all_nodes(ClusterMap)
            ),

            % If any, kill some server nodes after the specified time
            maps:foreach(
                fun(Server, FailAfter) ->
                    erlang:spawn(
                        fun() ->
                            ok = timer:sleep(FailAfter),
                            stop_single_server(ConfigFile, Server)
                        end
                    )
                end,
                FailureSpec
            ),

            %% Wait at least the same time that the benchmark is supposed to run
            BenchTimeout = timer:minutes(RunsForMinutes) + ?TIMEOUT,
            pmap(
                fun({Replica, Node}) ->
                    NodeStr = atom_to_list(Node),
                    RunConfigPath = filename:join(
                        home_path_for_node(NodeStr),
                        "run.config"
                    ),
                    Command = erlang_client_command(
                        NodeStr,
                        "run",
                        RunConfigPath,
                        Master,
                        integer_to_list(MasterPort),
                        atom_to_list(Replica)
                    ),
                    Cmd = io_lib:format("~s \"~s\" ~s", [?IN_NODES_PATH, Command, NodeStr]),
                    safe_cmd(Cmd)
                end,
                NodesWithReplicas,
                BenchTimeout
            ),

            %% Ensure that measurements have terminated
            %% TODO(borja): Since measure_cpu takes half the benchmark, this timeout should be tweaked.
            case async_for_receive(Token, ?TIMEOUT) of
                {error, timeout} ->
                    {error, timeout};
                _ ->
                    ok
            end
    end.

spawn_crasher(GitTag, Node, CrasherSpec) ->
    #{
        replica := CrasherReplica,

        master_node := MasterNode,
        master_port := MasterPort,

        magic_crash_key := CrashKey,
        hot_key := HotKey,

        op_timeout := OpTimeoutSpec,
        commit_timeout := CommitTimeoutSpec,

        value_bytes := ValueBytes
    } = CrasherSpec,

    {ok, OpTimeout} = parse_timeout_spec(OpTimeoutSpec),
    {ok, CommitTimeout} = parse_timeout_spec(CommitTimeoutSpec),

    ArgString = io_lib:format(
        "-replica ~s -master_ip ~s -master_port ~b -crashKey ~b -hotKey ~b -opTimeout ~s -commitTimeout ~s -value_bytes ~b",
        [CrasherReplica, atom_to_list(MasterNode), MasterPort, CrashKey, HotKey, to_go_duration(OpTimeout), to_go_duration(CommitTimeout), ValueBytes]
    ),

    Command = client_command(
        atom_to_list(Node),
        GitTag,
        "crasher",
        ArgString
    ),

    Cmd = io_lib:format("~s \"~s\" ~s", [?IN_NODES_PATH, Command, atom_to_list(Node)]),
    safe_cmd(Cmd),

    ok.

cleanup_master(Master) ->
    io:format("~p~n", [do_in_nodes_seq("rm -rf /home/borja.deregil/sources; mkdir -p /home/borja.deregil/sources", [Master])]),
    ok.

cleanup_servers(ClusterMap) ->
    ServerNodes = server_nodes(ClusterMap),
    io:format("~p~n", [do_in_nodes_par("rm -rf /home/borja.deregil/sources; mkdir -p /home/borja.deregil/sources", ServerNodes, infinity)]),
    ok.

cleanup_clients(ClusterMap) ->
    ClientNodes = client_nodes(ClusterMap),
    Res = pmap(
        fun(Node) ->
            NodeStr = atom_to_list(Node),
            Home = home_path_for_node(NodeStr),
            Command = io_lib:format(
                "rm -rf ~s/sources; mkdir -p ~s/sources",
                [Home, Home]
            ),
            Cmd = io_lib:format("~s \"~s\" ~s", [?IN_NODES_PATH, Command, NodeStr]),
            safe_cmd(Cmd)
        end,
        ClientNodes,
        infinity
    ),
    io:format("~p~n", [Res]),
    ok.

pull_results(ConfigTerms, ConfigFile, ResultsFolder, RunTerms, ClusterMap, ShouldArchivePath) ->
    {NPartitions, NClients} =
        maps:fold(
            fun
                (_, #{servers := S, clients := C}, {0, 0}) -> {erlang:length(S), erlang:length(C)};
                (_, _, Acc) -> Acc
            end,
            {0, 0},
            ClusterMap
        ),
    OpToString =
        fun
            (read) -> io_lib:format("read_~b", [proplists:get_value(readonly_ops, RunTerms)]);
            (read_release) -> io_lib:format("read_release_~b", [proplists:get_value(readonly_ops, RunTerms)]);
            (read_distinct) -> io_lib:format("read_~b", [proplists:get_value(readonly_ops, RunTerms)]);
            (read_distinct_release) -> io_lib:format("read_~b", [proplists:get_value(readonly_ops, RunTerms)]);
            (read_distinct_measure) -> io_lib:format("read_~b", [proplists:get_value(readonly_ops, RunTerms)]);
            (read_distinct_no_commit) -> io_lib:format("read_~b", [proplists:get_value(readonly_ops, RunTerms)]);
            (read_random_pool) -> io_lib:format("read_~b", [proplists:get_value(readonly_ops, RunTerms)]);
            (read_track) -> io_lib:format("read_track_~b", [proplists:get_value(readonly_ops, RunTerms)]);
            (update) -> io_lib:format("update_~b", [proplists:get_value(writeonly_ops, RunTerms)]);
            (update_distinct) -> io_lib:format("update_~b", [proplists:get_value(writeonly_ops, RunTerms)]);
            (update_distinct_measure) -> io_lib:format("update_~b", [proplists:get_value(writeonly_ops, RunTerms)]);
            (update_release) -> io_lib:format("update_release_~b", [proplists:get_value(writeonly_ops, RunTerms)]);
            (update_track) -> io_lib:format("update_~b", [proplists:get_value(writeonly_ops, RunTerms)]);
            (update_retry) -> io_lib:format("update_~b", [proplists:get_value(writeonly_ops, RunTerms)]);
            (update_release_retry) -> io_lib:format("update_~b", [proplists:get_value(writeonly_ops, RunTerms)]);
            (update_track_wait) -> io_lib:format("update_~b", [proplists:get_value(writeonly_ops, RunTerms)]);
            (update_measure) -> io_lib:format("update_~b", [proplists:get_value(writeonly_ops, RunTerms)]);
            (mixed) ->
                R = proplists:get_value(readonly_ops, RunTerms),
                W = proplists:get_value(writeonly_ops, RunTerms),
                io_lib:format("mixed_~b_~b", [R, W]);
            (no_tx_read) ->
                io_lib:format("no_tx_read_~b", [proplists:get_value(readonly_ops, RunTerms)]);
            (no_tx_read_with_id) ->
                io_lib:format("no_tx_read_with_id_~b", [proplists:get_value(readonly_ops, RunTerms)]);
            (Other) ->
                atom_to_list(Other)
        end,
    OpString =
        lists:foldl(
            fun
                % Old format
                ({Op, _}, "") -> io_lib:format("op_~s", [OpToString(Op)]);
                ({Op, _}, Acc) -> io_lib:format("~s+op_~s", [Acc, OpToString(Op)]);
                % New format
                (Op, "") -> io_lib:format("op_~s", [OpToString(Op)]);
                (Op, Acc) -> io_lib:format("~s+op_~s", [Acc, OpToString(Op)])
            end,
            "",
            proplists:get_value(operations, RunTerms, [])
        ),
    Path = io_lib:format(
        "partitions_~b+cl_~b+cm_~b+poolSize_~b+~s+t_~b_~s",
        [
            NPartitions,
            maps:size(ClusterMap),
            NClients,
            proplists:get_value(tcp_pool_size, ConfigTerms, "NA"),
            case OpString of "" -> "op_NA"; _ -> OpString end,
            proplists:get_value(concurrent, RunTerms, "NA"),
            calendar:system_time_to_rfc3339(erlang:system_time(millisecond), [{unit, millisecond}])
        ]
    ),
    pull_results_to_path(ets:lookup_element(?CONF, client_variant, 2), ConfigFile, ClusterMap, filename:join(ResultsFolder, Path), ShouldArchivePath).

pull_results_to_path(ClientVariant, ConfigFile, ClusterMap, Path, ShouldArchivePath) ->
    GitTag = ets:lookup_element(?CONF, ext_tag, 2),
    PullClients = fun(Timeout) ->
        pmap(
            fun(Node) ->
                NodeStr = atom_to_list(Node),
                HomePathForNode = home_path_for_node(NodeStr),
                TargetPath = filename:join([?RESULTS_DIR, Path, NodeStr]),

                safe_cmd(io_lib:format("mkdir -p ~s", [TargetPath])),

                CompressCmd =
                    case ClientVariant of
                        go_runner ->
                            ResultPath = io_lib:format("~s/runner_results/current", [home_path_for_node(NodeStr)]),
                            client_command(NodeStr, GitTag, "compress", ResultPath);
                        lasp_bench_runner ->
                            erlang_client_command(NodeStr, "compress")
                    end,

                %% Compress the results before returning, speeds up transfer
                _ = do_in_nodes_seq(CompressCmd, [Node]),

                %% Transfer results (-C compresses on flight)
                safe_cmd(io_lib:format(
                    "scp -C -i ~s borja.deregil@~s:~s/results.tar.gz ~s",
                    [?SSH_PRIV_KEY, NodeStr, HomePathForNode, TargetPath]
                )),

                safe_cmd(io_lib:format(
                    "scp -i ~s borja.deregil@~s:~s/~s ~s",
                    [?SSH_PRIV_KEY, NodeStr, HomePathForNode, ConfigFile, TargetPath]
                )),

                %% Transfer CPU load file
                safe_cmd(io_lib:format(
                    "scp -i ~s borja.deregil@~s:~s/~s.cpu ~s",
                    [?SSH_PRIV_KEY, NodeStr, HomePathForNode, NodeStr, TargetPath]
                )),

                %% Rename configuration to cluster.config
                safe_cmd(io_lib:format(
                    "cp ~s ~s/cluster.config",
                    [filename:join(TargetPath, ConfigFile), TargetPath]
                )),

                %% Uncompress results
                safe_cmd(io_lib:format(
                    "tar -xzf ~s/results.tar.gz -C ~s --strip-components 1",
                    [TargetPath, TargetPath]
                )),

                ok
            end,
            client_nodes(ClusterMap),
            Timeout
        )
    end,

    PullServerLogs = fun(Timeout) ->
        pmap(
            fun(Node) ->
                NodeStr = atom_to_list(Node),
                HomePathForNode = home_path_for_node(NodeStr),
                % Prefix folder with '_' to be excluded later
                TargetPath = filename:join([?RESULTS_DIR, Path, io_lib:format("_~s", [NodeStr])]),

                safe_cmd(io_lib:format("mkdir -p ~s", [TargetPath])),

                %% Transfer logs (-C compresses on flight)
                safe_cmd(io_lib:format(
                    "scp -C -i ~s borja.deregil@~s:~s/~s.log ~s",
                    [?SSH_PRIV_KEY, NodeStr, HomePathForNode, NodeStr, TargetPath]
                )),

                %% Transfer CPU load file
                safe_cmd(io_lib:format(
                    "scp -i ~s borja.deregil@~s:~s/~s.cpu ~s",
                    [?SSH_PRIV_KEY, NodeStr, HomePathForNode, NodeStr, TargetPath]
                )),

                %% Transfer screenlog file
                safe_cmd(io_lib:format(
                    "scp -i ~s borja.deregil@~s:~s/screenlog.0 ~s",
                    [?SSH_PRIV_KEY, NodeStr, HomePathForNode, TargetPath]
                )),

                case ets:lookup(?CONF, cpu_profile) of
                    [{cpu_profile, CPUPath}] ->
                        %% Transfer cpu profile file, if it exists
                        safe_cmd(io_lib:format(
                            "scp -C -i ~s borja.deregil@~s:~s/~s ~s",
                            [?SSH_PRIV_KEY, NodeStr, HomePathForNode, CPUPath, TargetPath]
                        )),

                        %% We also need the binary that produced the profile file
                        safe_cmd(io_lib:format(
                            "scp -C -i ~s borja.deregil@~s:~s/sources/~s/server_linux_amd64 ~s",
                            [?SSH_PRIV_KEY, NodeStr, HomePathForNode, GitTag, TargetPath]
                        ));
                    _ ->
                        io:format("No cpu profile found~n"),
                        ok
                end,

                ok
            end,
            server_nodes(ClusterMap),
            Timeout
        )
    end,

    DoFun = fun(Timeout) ->
        case PullClients(Timeout) of
            {error, _} ->
                error;
            _ ->
                PullServerLogs(Timeout)
        end
    end,

    case DoFun(?TIMEOUT) of
        error ->
            error;
        _ ->
            case ShouldArchivePath of
                false ->
                    %% This experiment is still on-going, don't archive the path
                    ok;
                {archive, PathToArchive} ->
                    %% Compress everything into a single archive file
                    safe_cmd(io_lib:format(
                        "./archive_results.sh ~s",
                        [filename:join(?RESULTS_DIR, PathToArchive)]
                    )),
                    ok
            end
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Show Latencies
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_config_key(Key, Config, Default) ->
    case lists:keyfind(Key, 1, Config) of
        false -> Default;
        {Key, Value} -> Value
    end.

dedup_specs(Specs) ->
    dedup_specs(Specs, #{}, []).

dedup_specs([], _, Acc) ->
    lists:reverse(Acc);

dedup_specs([Spec | Rest], Prev, Acc0) ->
    #{
        results_folder := ExperimentName,
        config_terms := ConfigTerms
    } = Spec,
    PrevName = maps:get(results_folder, Prev, ""),
    Normalized = lists:usort(ConfigTerms),
    PrevNormalized = lists:usort(maps:get(config_terms, Prev, [])),
    Acc =
        if
            (ExperimentName =/= PrevName) orelse (Normalized =/= PrevNormalized) ->
                [Spec | Acc0];
            true ->
                Acc0
        end,
    dedup_specs(Rest, Spec, Acc).

show_latencies([]) ->
    ok;

show_latencies([ #{ results_folder := ExpName, config_terms := Config } | Rest]) ->
    FaultTolerance = get_config_key(fault_tolerance_factor, Config, 1),
    QuorumSize = FaultTolerance + 1,
    {_, Leaders} = lists:keyfind(leaders, 1, Config),
    {_, Latencies} = lists:keyfind(latencies, 1, Config),
    if
        map_size(Latencies) == 0 ->
            show_latencies(Rest);

        true ->
            TotalPartitions = map_size(Leaders),
            case lists:usort(maps:values(Leaders)) of
                [Leader] ->
                    show_latencies_for_partition(ExpName, TotalPartitions, undefined, Leader, Latencies, QuorumSize);
                _ ->
                    lists:foreach(
                        fun({Partition, Leader}) ->
                            show_latencies_for_partition(ExpName, TotalPartitions, Partition, Leader, Latencies, QuorumSize)
                        end,
                        maps:to_list(Leaders)
                    )
            end,
            show_latencies(Rest)
    end.

show_latencies_for_partition(ExpName, TotalPartitions, Partition, Leader, Latencies, QuorumSize) ->
    PartitionStr =
        case Partition of
            undefined -> "all";
            _ -> io_lib:format("~b", [Partition])                
        end,
    lists:foreach(
        fun(Region) ->
            MinCommitLat = compute_commit_latency(Region, Leader, Latencies, QuorumSize),
            MinDeliveryLat = 2 * to_leader_latency(Region, Leader, Latencies),
            io:format(
                "~s, ~b, ~b ~s, ~s, ~b, ~b\n",
                [ExpName, TotalPartitions, QuorumSize, PartitionStr, Region, MinCommitLat, MinDeliveryLat]
            )
        end,
        maps:keys(Latencies)
    ).

-spec compute_commit_latency(atom(), atom(), latencies(), non_neg_integer()) -> non_neg_integer().
compute_commit_latency(Leader, Leader, _, 1) ->
    0;
compute_commit_latency(Leader, Leader, Latencies, Q) ->
    compute_minimal_quorum_rtt(Leader, Latencies, Q);
compute_commit_latency(Region, LeaderRegion, Latencies, Q) when Region =/= LeaderRegion ->
    compute_follower_latency(Region, LeaderRegion, Latencies, Q).

-spec compute_minimal_quorum_rtt(
    At :: atom(),
    Latencies :: latencies(),
    QuorumSize :: non_neg_integer()
) -> non_neg_integer().

compute_minimal_quorum_rtt(At, Latencies, Q) ->
    %% Q - 1 to skip ourselves
    compute_minimal_quorum_rtt(At, Latencies, Q - 1, min_half_rtt(At, Latencies, 0)).

-spec compute_minimal_quorum_rtt(
    At :: atom(),
    Latencies :: latencies(),
    N :: non_neg_integer(),
    MinLinks :: {non_neg_integer(), non_neg_integer()}
) -> non_neg_integer().

compute_minimal_quorum_rtt(At, Latencies, Q, {MinCount, MinSoFar}) ->
    case Q of
        1 ->
            MinSoFar * 2;
        N when N =< MinCount ->
            %% Quorum size is less than the amount of paths with the same latency, so we know we will
            %% commit with that latency.
            MinSoFar * 2;
        _ ->
            %% We can skip MinCount replicas, they all have the same latency.
            %% We know that there's at least one replica left to get a quorum
            compute_minimal_quorum_rtt(
                At,
                Latencies,
                (Q - MinCount),
                min_half_rtt(At, Latencies, MinSoFar)
            )
    end.

-spec compute_follower_latency(atom(), atom(), latencies(), non_neg_integer()) -> non_neg_integer().
compute_follower_latency(At, Leader, Latencies, Q) ->
    ToLeader = to_leader_latency(At, Leader, Latencies),
    G = config_to_digraph(Latencies),
    compute_follower_latency(At, Leader, G, Q - 1, ToLeader, shortest_latency(Leader, At, G, 0)).

-spec compute_follower_latency(
    At :: atom(),
    Leader :: atom(),
    LatGraph :: latencies_graph(),
    QuorumSize :: non_neg_integer(),
    ToLeaderLatency :: non_neg_integer(),
    MinLinks :: {non_neg_integer(), non_neg_integer()}
) -> non_neg_integer().

compute_follower_latency(At, Leader, G, Q, ToLeader, {MinCount, MinSoFar}) ->
    case Q of
        1 ->
            MinSoFar + ToLeader;
        M when M =< MinCount ->
            MinSoFar + ToLeader;
        _ ->
            compute_follower_latency(
                At,
                Leader,
                G,
                (Q - MinCount),
                ToLeader,
                shortest_latency(Leader, At, G, MinSoFar)
            )
    end.

% @doc Get the latency of the min path from `At` to any other site, but bigger than `Min`.
%  If X paths have the same latency M, return {X, M}
-spec min_half_rtt(atom(), latencies(), non_neg_integer()) ->
    {non_neg_integer(), non_neg_integer()}.
min_half_rtt(At, Latencies, Threshold) ->
    lists:foldl(
        fun
            ({_, Lat}, {Count, MinLat}) when Lat =< MinLat andalso Lat > Threshold ->
                {Count + 1, Lat};
            (_, Acc) ->
                Acc
        end,
        {0, undefined},
        maps:get(At, Latencies)
    ).

-spec to_leader_latency(atom(), atom(), latencies()) -> non_neg_integer().
to_leader_latency(Leader, Leader, _) ->
    0;
to_leader_latency(Region, Leader, Latencies) ->
    #{Region := Targets} = Latencies,
    {Leader, LatencyToLeader} = lists:keyfind(Leader, 1, Targets),
    LatencyToLeader.

-spec config_to_digraph(latencies()) -> latencies_graph().
config_to_digraph(Latencies) ->
    G = digraph:new(),
    _ = [digraph:add_vertex(G, K) || K <- maps:keys(Latencies)],
    maps:fold(
        fun(K, Targets, Acc) ->
            [digraph:add_edge(G, K, T, L) || {T, L} <- Targets],
            Acc
        end,
        G,
        Latencies
    ).

-spec shortest_latency(
    From :: atom(),
    To :: atom(),
    Digraph :: latencies_graph(),
    Min :: non_neg_integer()
) -> {non_neg_integer(), non_neg_integer()}.

shortest_latency(From, To, Digraph, Min) ->
    shortest_latency(From, To, Digraph, #{From => []}, 0, Min).

-spec shortest_latency(
    From :: atom(),
    To :: atom(),
    Digraph :: latencies_graph(),
    Visited :: #{atom() => []},
    Cost :: non_neg_integer(),
    Min :: non_neg_integer()
) -> non_neg_integer().

shortest_latency(From, To, Digraph, Visited, Cost, Min) ->
    lists:foldl(
        fun(E, {Count, Acc}) ->
            {ChildCount, Total} =
                case digraph:edge(Digraph, E) of
                    {_, From, To, Lat} ->
                        {1, Cost + Lat};
                    {_, From, Neigh, Lat} when not is_map_key(Neigh, Visited) ->
                        shortest_latency(
                            Neigh,
                            To,
                            Digraph,
                            Visited#{Neigh => []},
                            Cost + Lat,
                            Min
                        );
                    _ ->
                        {0, Acc}
                end,
            if
                Total =< Acc andalso Total > Min ->
                    {Count + ChildCount, Total};
                true ->
                    {Count, Acc}
            end
        end,
        {0, undefined},
        digraph:out_edges(Digraph, From)
    ).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Show Nodes
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

aggregate_nodes(Specs) ->
    aggregate_nodes(Specs, sets:new()).

aggregate_nodes([], Acc) ->
    Acc;

aggregate_nodes([ #{config_terms := ConfigTerms} | Rest ], Acc) ->
    {master_node, Master} = lists:keyfind(master_node, 1, ConfigTerms),
    {clusters, ClusterMap} = lists:keyfind(clusters, 1, ConfigTerms),

    Servers = sets:from_list(server_nodes(ClusterMap)),
    Clients = sets:from_list(client_nodes(ClusterMap)),

    aggregate_nodes(Rest, sets:union([sets:add_element(Master, Acc), Servers, Clients])).

format_node_set(Set) ->
    [Head | Tail] = lists:usort(sets:to_list(Set)),
    lists:foldl(
        fun(Elt, Acc) ->
            io_lib:format("~s~n~s", [Acc, Elt])
        end,
        io_lib:format("~s", [Head]),
        Tail
    ).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Util
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec home_path_for_node(string()) -> string().
home_path_for_node(NodeStr) ->
    {ok, MP} = re:compile("apollo-*"),
    case re:run(NodeStr, MP) of
        {match, _} ->
            "/home/borja.deregil";
        nomatch ->
            % Must be veleta
            ?VELETA_HOME
    end.

master_command(GitTag, Command) ->
    io_lib:format("./master.sh -T ~s ~s", [GitTag, Command]).

master_command(GitTag, Command, Arg1) ->
    io_lib:format("./master.sh -T ~s ~s ~s", [GitTag, Command, Arg1]).

master_command(GitTag, Command, Arg1, Arg2, Arg3) ->
    io_lib:format("./master.sh -T ~s ~s ~s ~s ~s", [GitTag, Command, Arg1, Arg2, Arg3]).

server_command(ConfigFile, Command) ->
    io_lib:format("./server.escript -v -f /home/borja.deregil/~s -c ~s", [ConfigFile, Command]).

server_command(ConfigFile, Command, Arg) ->
    io_lib:format("./server.escript -v -f /home/borja.deregil/~s -c ~s=~s", [
        ConfigFile,
        Command,
        Arg
    ]).

server_command(ConfigFile, Command, Replica, Partition) ->
    io_lib:format("./server.escript -r ~s -p ~b -v -f /home/borja.deregil/~s -c ~s", [
        Replica,
        Partition,
        ConfigFile,
        Command
    ]).

client_command(NodeStr, GitTag, Command, Arg1) ->
    HomePath = home_path_for_node(NodeStr),
    io_lib:format(
        "~s/bench.sh -H ~s -T ~s ~s ~s",
        [HomePath, HomePath, GitTag, Command, Arg1]
    ).

client_command(NodeStr, GitTag, Command, Arg1, Arg2, Arg3, Arg4, Arg5) ->
    HomePath = home_path_for_node(NodeStr),
    io_lib:format(
        "~s/bench.sh -H ~s -T ~s ~s ~s ~s ~s ~s ~s",
        [HomePath, HomePath, GitTag, Command, Arg1, Arg2, Arg3, Arg4, Arg5]
    ).

erlang_client_command(NodeStr, Command) ->
    HomePath = home_path_for_node(NodeStr),
    io_lib:format(
        "~s/erlang_bench.sh -H ~s ~s",
        [HomePath, HomePath, Command]
    ).

erlang_client_command(NodeStr, Command, Arg1, Arg2, Arg3, Arg4) ->
    HomePath = home_path_for_node(NodeStr),
    io_lib:format(
        "~s/erlang_bench.sh -H ~s ~s ~s ~s ~s ~s",
        [HomePath, HomePath, Command, Arg1, Arg2, Arg3, Arg4]
    ).

erlang_client_command(NodeStr, Command, Arg1, Arg2, Arg3, Arg4, Arg5) ->
    HomePath = home_path_for_node(NodeStr),
    io_lib:format(
        "~s/erlang_bench.sh -H ~s ~s ~s ~s ~s ~s ~s",
        [HomePath, HomePath, Command, Arg1, Arg2, Arg3, Arg4, Arg5]
    ).

transfer_script(Node, File) ->
    transfer_from(Node, ?SELF_DIR, File).

transfer_config(Node, File) ->
    transfer_from(Node, ?CONFIG_DIR, File).

transfer_from(Node, Path, File) ->
    NodeStr = atom_to_list(Node),
    Cmd = io_lib:format(
        "scp -i ~s ~s/~s borja.deregil@~s:~s",
        [?SSH_PRIV_KEY, Path, File, NodeStr, home_path_for_node(NodeStr)]
    ),
    safe_cmd(Cmd).

all_nodes(Map) ->
    lists:usort(lists:flatten([S ++ C || #{servers := S, clients := C} <- maps:values(Map)])).

server_nodes(Map) ->
    lists:usort(lists:flatten([N || #{servers := N} <- maps:values(Map)])).

server_nodes_with_partitions(Replica, Map) ->
    #{servers := N} = maps:get(Replica, Map),
    Sorted = lists:usort(N),
    {
        Sorted,
        maps:from_list(lists:zip(Sorted, lists:seq(0, length(Sorted)-1)))
    }.

client_nodes(Map) ->
    lists:usort(lists:flatten([N || #{clients := N} <- maps:values(Map)])).

do_in_nodes_seq(Command, Nodes) ->
    Cmd = io_lib:format("~s \"~s\" ~s", [?IN_NODES_PATH, Command, list_to_str(Nodes)]),
    safe_cmd(Cmd).

do_in_nodes_par(Command, Nodes, Timeout) ->
    pmap(
        fun(Node) ->
            Cmd = io_lib:format("~s \"~s\" ~s", [?IN_NODES_PATH, Command, atom_to_list(Node)]),
            safe_cmd(Cmd)
        end,
        Nodes,
        Timeout
    ).

do_in_nodes_par_func(Func, Nodes, Timeout) ->
    pmap(
        fun(Node) ->
            NodeStr = atom_to_list(Node),
            Command = Func(NodeStr),
            Cmd = io_lib:format("~s \"~s\" ~s", [?IN_NODES_PATH, Command, NodeStr]),
            safe_cmd(Cmd)
        end,
        Nodes,
        Timeout
    ).

list_to_str(Nodes) ->
    lists:foldl(fun(Elem, Acc) -> Acc ++ io_lib:format("~s ", [Elem]) end, "", Nodes).

safe_cmd(Cmd) ->
    case get_conf(silent, false) of
        true -> ok;
        false -> ok = io:format("~s~n", [Cmd])
    end,
    case get_conf(dry_run, false) of
        true -> "";
        false -> os:cmd(Cmd)
    end.

get_conf(Key, Default) ->
    case ets:lookup(?CONF, Key) of
        [] -> Default;
        [{Key, Val}] -> Val
    end.

pmap(F, L, infinity) ->
    Parent = self(),
    lists:foldl(
        fun(X, N) ->
            erlang:spawn(fun() -> Parent ! {pmap, N, F(X)} end),
            N + 1
        end,
        0,
        L
    ),
    L2 = [ receive {pmap, N, R} -> {N, R} end || _ <- L ],
    [R || {_, R} <- lists:keysort(1, L2)];

pmap(F, L, Timeout) ->
    Parent = self(),
    {Pids, _} = lists:foldl(
        fun(X, {Pids, N}) ->
            Pid = erlang:spawn(fun() -> Parent ! {pmap, N, F(X)} end),
            {[Pid | Pids], N + 1}
        end,
        {[], 0},
        L
    ),
    try
        L2 = [ receive {pmap, N, R} -> {N, R} after Timeout -> throw(timeout) end || _ <- L ],
        [R || {_, R} <- lists:keysort(1, L2)]
    catch throw:timeout ->
        [ erlang:exit(P, kill) || P <- Pids ],
        {error, timeout}
    end.

async_for(F, L) ->
    Parent = self(),
    {Pids, _} = lists:foldl(
        fun(X, {Pids, N}) ->
            Pid = erlang:spawn(fun() -> Parent ! {async_for, N, F(X)} end),
            {[Pid | Pids], N + 1}
        end,
        {[], 0},
        L
    ),
    Pids.

async_for_receive(Pids, infinity) ->
    L2 = [ receive {async_for, N, R} -> {N, R} end || _ <- Pids ],
    [R || {_, R} <- lists:keysort(1, L2)];
async_for_receive(Pids, Timeout) ->
    try
        L2 = [ receive {async_for, N, R} -> {N, R} after Timeout -> throw(timeout) end || _ <- Pids ],
        [R || {_, R} <- lists:keysort(1, L2)]
    catch throw:timeout ->
        [ erlang:exit(P, kill) || P <- Pids ],
        {error, timeout}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% util
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec write_terms(string(), [{atom(), term()}]) -> ok | {error, term()}.
write_terms(FileName, Terms) ->
    Format = fun(Term) -> io_lib:format("~tp.~n", [Term]) end,
    Text = unicode:characters_to_binary(lists:map(Format, Terms)),
    file:write_file(FileName, Text).

to_go_duration(TimeMs) -> io_lib:format("~bms", [TimeMs]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% getopt
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

parse_args([]) ->
    {error, noargs};
parse_args(Args) ->
    case parse_args(Args, #{}) of
        {ok, Opts} -> required(Opts);
        Err -> Err
    end.

parse_args([], Acc) ->
    {ok, Acc};
parse_args([[$- | Flag] | Args], Acc) ->
    case Flag of
        [$s] ->
            parse_args(Args, Acc#{silent => true});
        [$d] ->
            parse_args(Args, Acc#{dry_run => true});
        "-generate" ->
            parse_flag(Flag, Args, fun(Arg) -> Acc#{generate_definition => Arg} end);
        "-latencies" ->
            parse_flag(Flag, Args, fun(Arg) -> Acc#{show_latencies => Arg} end);
        "-nodes" ->
            parse_flag(Flag, Args, fun(Arg) -> Acc#{show_nodes => Arg} end);
        "-experiment" ->
            parse_flag(Flag, Args, fun(Arg) -> Acc#{experiment_definition => Arg} end);
        [$h] ->
            usage(),
            halt(0);
        _ ->
            {error, io_lib:format("badarg ~p", [Flag])}
    end;
parse_args(_, _) ->
    {error, "noarg"}.

parse_flag(Flag, Args, Fun) ->
    case Args of
        [FlagArg | Rest] -> parse_args(Rest, Fun(FlagArg));
        _ -> {error, io_lib:format("noarg ~p", [Flag])}
    end.

required(Opts) ->
    case Opts of
        #{generate_definition := _} ->
            {ok, Opts};
        #{experiment_definition := _} ->
            {ok, Opts};
        #{show_latencies := _} ->
            {ok, Opts};
        #{show_nodes := _} ->
            {ok, Opts};
        _ ->
            {error, "Missing required --nodes, --latencies, --generate or --experiment flags"}
    end.
