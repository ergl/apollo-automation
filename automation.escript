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
        'apollo-2-7.imdea',
        'apollo-2-8.imdea',
        'apollo-2-9.imdea',
        'apollo-2-10.imdea',
        'apollo-2-11.imdea',
        'apollo-2-12.imdea'
    ]
).

% These nodes should be configured manually
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
        "Usage: ~s [-ds] --latencies <experiment-definition> | --generate <experiment-definition> | --experiment <experiment-definition>~n",
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
            Specs0 = materialize_experiments(?LOCAL_CONFIG_DIR, DefinitionTerms),
            Specs1 = dedup_specs_for_latencies(Specs0),
            io:format(
                "experiment, partitions, quorum_size, at_partition, at_region, commit_latency, delivery_latency\n"
            ),
            show_latencies(Specs1);

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

        Ops = maps:get(operations, RunWith),
        lists:foreach(
            fun(Op) ->
                case Op of
                    read -> VerifyOp(Op, [readonly_ops]);
                    read_random_pool -> VerifyOp(Op, [readonly_ops]);
                    read_distinct -> VerifyOp(Op, [readonly_ops]);
                    update -> VerifyOp(Op, [writeonly_ops]);
                    update_distinct -> VerifyOp(Op, [writeonly_ops]);
                    mixed -> VerifyOp(Op, [readonly_ops, writeonly_ops]);
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

        % Fill all template values from experiment definition
        ExperimentTerms =
            maps:fold(ReplaceKeyFun, TermsWithConcurrent, RunWith),

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

        [
            #{
                config_terms => ConfigTerms,
                results_folder => maps:get(results_folder, Experiment),
                run_terms => ExperimentTerms,
                load_spec => LoadSpec
            }
        ].

materialize_cluster_definition(RunOnTerms, TemplateTerms) ->
    SimpleReplacements = maps:without([leaders, clusters, partitions, per_partition], RunOnTerms),

    Replicas = maps:get(clusters, RunOnTerms),
    Clusters =
        build_cluster_map(
            Replicas,
            maps:get(partitions, RunOnTerms),
            maps:get(per_partition, RunOnTerms)
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

build_cluster_map(Clusters, NPartitions, NClients) ->
    RealClients =
        case NClients of
            auto ->
                AvailableForClients =
                    (length(?ALL_NODES) + maps:size(?BIG_NODES)) -
                        (NPartitions * length(Clusters)),
                AvailableForClients div length(Clusters);
            _ ->
                trunc(NPartitions * NClients)
        end,
    build_cluster_map(Clusters, NPartitions, RealClients, #{}, ?ALL_NODES, lists:sort(maps:keys(?BIG_NODES))).

build_cluster_map([], _, _, Acc, _, _) ->
    Acc;
build_cluster_map([ClusterName | Rest], NP, NClients, Acc, Available0, PreferenceClients0)
    when not is_map_key(ClusterName, Acc) ->
        {Servers, Available1} = lists:split(NP, Available0),
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
            io:fwrite(
                standard_error,
                "Retrying spec error ~p (~b/~b) on spec: ~n~p~n", [Reason, Retries, ?RETRIES, Spec]
            ),
            %% Retry again, with last cluster as undefined so that we can start from a clean slate
            run_experiments(Retries - 1, Opts, undefined, AllSpecs);

        {error, Reason} ->
            io:fwrite(standard_error, "Spec error on ~p~nError:~p~n", [Spec, Reason]),
            error;

        {fatal_error, Reason} ->
            io:fwrite(standard_error, "Fatal spec error on ~p~nError: ~p~n", [Spec, Reason]),
            error
    end.

execute_spec(Opts, PrevConfigTerms, Spec, NextConfigTerms, NextResults) ->
    #{
        config_terms := ConfigTerms,
        results_folder := Results,
        run_terms := RunTerms,
        load_spec := LoadSpec
    } = Spec,

    _ = ets:new(?CONF, [set, named_table]),
    case catch preprocess_args(Opts, ConfigTerms) of
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
                            ok = download_runner(ClusterMap),

                            %% Set up any needed latencies
                            ok = setup_latencies(ConfigFile, ClusterMap)
                    end,

                    %% Start things, re-sync NTP
                    ok = sync_nodes(Master, ClusterMap),
                    ok = start_master(Master),
                    ok = start_server(ConfigFile, ClusterMap),

                    %% Actual experiment: load then bench
                    ok = load_ext(Master, ClusterMap, LoadSpec),
                    ok = bench_ext(Master, RunTerms, ClusterMap),

                    %% Give system some time (1 sec) to stabilise
                    ok = timer:sleep(1000),

                    %% Gather all results from the experiment
                    %% If Results =/= NextResults, then we can archive the entire path
                    ShouldArchive =
                        case Results of
                            NextResults -> false;
                            _ -> {archive, Results}
                        end,
                    ok = pull_results(
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
                            ok = cleanup_latencies(ConfigFile, ClusterMap),
                            ok = cleanup_master(Master),
                            ok = cleanup_servers(ClusterMap),
                            ok = cleanup_clients(ClusterMap)
                    end,

                    ok
                catch
                    error:Exception:Stack ->
                        %% An exception happened, clean up everything just in case
                        brutal_client_kill(ClusterMap),
                        cleanup_latencies(ConfigFile, ClusterMap),
                        cleanup_master(Master),
                        cleanup_servers(ClusterMap),
                        cleanup_clients(ClusterMap),
                        {error, {Exception, Stack}};

                    throw:Term:Stack ->
                        %% An exception happened, clean up everything just in case
                        brutal_client_kill(ClusterMap),
                        cleanup_latencies(ConfigFile, ClusterMap),
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

preprocess_args(Opts, ConfigTerms) ->
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
        false ->
            ok;
        {cpu_profile, ProfilePath} ->
            true = ets:insert(?CONF, {cpu_profile, ProfilePath})
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
                transfer_script(Node, "build_tc_rules.escript"),
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

start_master(Master) ->
    GitTag = ets:lookup_element(?CONF, ext_tag, 2),
    NumReplicas = ets:lookup_element(?CONF, n_replicas, 2),
    NumPartitions = ets:lookup_element(?CONF, n_partitions, 2),

    % Build the arguments for master
    LeaderSpec =
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

    case
        do_in_nodes_par(
                master_command(
                    GitTag,
                    "run",
                    integer_to_list(NumReplicas),
                    integer_to_list(NumPartitions),
                    LeaderSpec
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
            (ClusterName, #{servers := ServerNodes}, ok) ->
                case
                    do_in_nodes_par(server_command(ConfigFile, "start", atom_to_list(ClusterName)), lists:usort(ServerNodes), ?TIMEOUT)
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

brutal_client_kill(ClusterMap) ->
    _ = do_in_nodes_par("kill -9 \\$(pgrep -f runner_linux_amd64)", client_nodes(ClusterMap), ?TIMEOUT),
    ok.

download_runner(ClusterMap) ->
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
    end.

load_ext(Master, ClusterMap, LoadSpec) ->

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
    end.

bench_ext(Master, RunTerms, ClusterMap) ->
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
                    {report_interval, Seconds} ->
                        io_lib:format("~s -reportInterval ~bs", [Acc, Seconds]);
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
                        io_lib:format("~s -distribution 0", [Acc]);
                    {key_distribution, pareto} ->
                        io_lib:format("~s -distribution 1", [Acc]);
                    {operations, OpList} ->
                        lists:foldl(
                            fun(Op, InnerAcc) ->
                                io_lib:format("~s -operation ~s", [InnerAcc, atom_to_list(Op)])
                            end,
                            Acc,
                            OpList
                        );
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
    end.

-spec setup_latencies(_, _) -> ok | error.
setup_latencies(ConfigFile, ClusterMap) ->
    maps:fold(
        fun
            (_, _, error) ->
                error;

            (ClusterName, #{servers := ClusterServers}, ok) ->
                case
                    do_in_nodes_par(
                            server_command(ConfigFile, "tc", atom_to_list(ClusterName)),
                            ClusterServers,
                            ?TIMEOUT
                        )
                of
                    {error, _} ->
                        error;
                    Print ->
                        io:format("~p~n", [Print]),
                        ok
                end
        end,
        ok,
        ClusterMap
    ).

cleanup_latencies(ConfigFile, ClusterMap) ->
    maps:fold(
        fun(ClusterName, #{servers := ClusterServers}, _Acc) ->
            io:format(
                "~p~n",
                [
                    do_in_nodes_par(
                        server_command(ConfigFile, "tclean", atom_to_list(ClusterName)),
                        ClusterServers,
                        infinity
                    )
                ]
            )
        end,
        ok,
        ClusterMap
    ).

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

pull_results(ConfigFile, ResultsFolder, RunTerms, ClusterMap, ShouldArchivePath) ->
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
            (read_distinct) -> io_lib:format("read_~b", [proplists:get_value(readonly_ops, RunTerms)]);
            (read_random_pool) -> io_lib:format("read_~b", [proplists:get_value(readonly_ops, RunTerms)]);
            (update) -> io_lib:format("update_~b", [proplists:get_value(writeonly_ops, RunTerms)]);
            (update_distinct) -> io_lib:format("update_~b", [proplists:get_value(writeonly_ops, RunTerms)]);
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
        "partitions_~b+cl_~b+cm_~b+~s+t_~b_~s",
        [
            NPartitions,
            maps:size(ClusterMap),
            NClients,
            case OpString of "" -> "op_NA"; _ -> OpString end,
            proplists:get_value(concurrent, RunTerms, "NA"),
            calendar:system_time_to_rfc3339(erlang:system_time(millisecond), [{unit, millisecond}])
        ]
    ),
    pull_results_to_path(ConfigFile, ClusterMap, filename:join(ResultsFolder, Path), ShouldArchivePath).

pull_results_to_path(ConfigFile, ClusterMap, Path, ShouldArchivePath) ->
    GitTag = ets:lookup_element(?CONF, ext_tag, 2),
    PullClients = fun(Timeout) ->
        pmap(
            fun(Node) ->
                NodeStr = atom_to_list(Node),
                HomePathForNode = home_path_for_node(NodeStr),
                ResultPath = io_lib:format("~s/runner_results/current", [home_path_for_node(NodeStr)]),
                TargetPath = filename:join([?RESULTS_DIR, Path, NodeStr]),

                safe_cmd(io_lib:format("mkdir -p ~s", [TargetPath])),

                %% Compress the results before returning, speeds up transfer
                _ = do_in_nodes_seq(client_command(NodeStr, GitTag, "compress", ResultPath), [Node]),

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
                TargetPath = filename:join([?RESULTS_DIR, Path, NodeStr]),

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

dedup_specs_for_latencies(Specs) ->
    dedup_specs_for_latencies(Specs, #{}, []).

dedup_specs_for_latencies([], _, Acc) ->
    lists:reverse(Acc);

dedup_specs_for_latencies([Spec | Rest], Prev, Acc0) ->
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
    dedup_specs_for_latencies(Rest, Spec, Acc).

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
        _ ->
            {error, "Missing required --latencies, --generate or --experiment flags"}
    end.
