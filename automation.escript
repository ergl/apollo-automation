#!/usr/bin/env escript

-mode(compile).

-export([main/1]).

-define(SELF_DIR, "/home/borja.deregil/automation").
-define(SSH_PRIV_KEY, "/home/borja.deregil/.ssh/id_ed25519").
-define(RESULTS_DIR, "/home/borja.deregil/results").

-define(IN_NODES_PATH,
    unicode:characters_to_list(io_lib:format("~s/execute-in-nodes.sh", [?SELF_DIR]))
).

-define(CONFIG_DIR,
    unicode:characters_to_list(io_lib:format("~s/configuration", [?SELF_DIR]))
).

-define(TOKEN_CONFIG,
    unicode:characters_to_list(io_lib:format("~s/secret.config", [?CONFIG_DIR]))
).

-define(LASP_BENCH_BRANCH, "bench_ext").

-define(CONF, configuration).

% 2 minute timeout for pmap
-define(TIMEOUT, timer:minutes(2)).
-define(RETRIES, 5).

-type experiment_spec() :: #{config := string(), results_folder := string(), run_terms := [{atom(), term()}, ...]}.

usage() ->
    Name = filename:basename(escript:script_name()),
    ok = io:fwrite(
        standard_error,
        "Usage: ~s [-ds] --experiment <experiment-definition>~n",
        [Name]
    ).

main(Args) ->
    case parse_args(Args) of
        {error, Reason} ->
            io:fwrite(standard_error, "Wrong option: reason ~s~n", [Reason]),
            usage(),
            halt(1);
        {ok, Opts = #{experiment_definition := Definition}} ->
            {ok, DefinitionTerms} = file:consult(Definition),
            Specs = materialize_experiments(DefinitionTerms),
            run_experiments(Opts, Specs)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Parse, materialize experiments
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec materialize_experiments([{atom(), term()}, ...]) -> [experiment_spec()].
materialize_experiments(Definition) ->
    {run_template, RunTemplate} = lists:keyfind(run_template, 1, Definition),
    {experiments, Experiments} = lists:keyfind(experiments, 1, Definition),
    {ok, Terms} = file:consult(filename:join([?CONFIG_DIR, RunTemplate])),
    %% Don't use flatmap, only flattens one level deep
    lists:flatten(
        lists:map(
            fun(Exp) -> materialize_single_experiment(Terms, Exp) end,
            Experiments
        )
    ).

materialize_single_experiment(Terms, Exp = #{clients := {M,F,A}}) ->
    [ materialize_single_experiment(Terms, Exp#{clients => N}) || N <- apply(M, F, A) ];

materialize_single_experiment(Terms, Exp = #{clients := List})
    when is_list(List) ->
        [ materialize_single_experiment(Terms, Exp#{clients => N}) || N <- List ];

materialize_single_experiment(TemplateTerms, Experiment = #{clients := N})
    when is_integer(N) ->
        %% Sanity check
        Workers = erlang:max(N, 1),

        % Set our number of threads
        TermsWithConcurrent = lists:keyreplace(concurrent, 1, TemplateTerms, {concurrent, Workers}),

        % Fill all template values from experiment definition
        ExperimentTerms =
            maps:fold(
                fun(Key, Value, Acc) ->
                    lists:keyreplace(Key, 1, Acc, {Key, Value})
                end,
                TermsWithConcurrent,
                maps:get(run_with, Experiment)
            ),

        [
            #{
                config => filename:join(?CONFIG_DIR, maps:get(config_file, Experiment)),
                results_folder => maps:get(results_folder, Experiment),
                run_terms => ExperimentTerms
            }
        ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Prepare experiment
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec cluster_config(experiment_spec()) -> string() | undefined.
cluster_config(#{config := Config}) -> Config;
cluster_config(_) -> undefined.

-spec get_next_cluster_config([experiment_spec()]) -> string() | undefined.
get_next_cluster_config([]) -> undefined;
get_next_cluster_config([Head | _]) -> cluster_config(Head).

-spec get_next_result_folder([experiment_spec()]) -> string() | undefined.
get_next_result_folder([ #{results_folder := Results} | _]) -> Results;
get_next_result_folder(_) -> undefined.

run_experiments(Opts, Specs) ->
    run_experiments(?RETRIES, Opts, undefined, Specs).

run_experiments(_, _, _, []) ->
    ok;

run_experiments(Retries, Opts, LastCluster, [ Spec | Rest ]=AllSpecs) ->
    Result = execute_spec(
        Opts,
        LastCluster,
        Spec,
        get_next_cluster_config(Rest),
        get_next_result_folder(Rest)
    ),
    case Result of
        ok ->
            %% Start next spec with fresh retries
            run_experiments(?RETRIES, Opts, cluster_config(Spec), Rest);

        {error, _} when Retries > 0 ->
            io:fwrite(
                standard_error,
                "Retrying spec error (~b/~b) on spec: ~n~p~n", [Retries, ?RETRIES, Spec]
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

execute_spec(Opts, PrevConfig, Spec, NextConfig, NextResults) ->
    #{config := ConfigFile, results_folder := Results, run_terms := RunTerms} = Spec,

    _ = ets:new(?CONF, [set, named_table]),
    case catch preprocess_args(Opts, ConfigFile) of
        {'EXIT', TraceBack} ->
            ets:delete(?CONF),
            {fatal_error, TraceBack};

        {ClusterMap, Master} ->
            Result =
                try
                    case ConfigFile =:= PrevConfig of
                        true ->
                            %% We're reusing the same cluster, no need to download anything.
                            %% Just check if something went wrong.
                            ok = check_nodes(Master, ClusterMap);
                        false ->
                            %% This is a new cluster, past spec cleaned up, so we need to re-download things
                            ok = check_nodes(Master, ClusterMap),
                            ok = push_scripts(filename:basename(ConfigFile), Master, ClusterMap),

                            ok = download_master(Master),
                            ok = download_server(filename:basename(ConfigFile), ClusterMap),
                            ok = download_lasp_bench(ClusterMap),

                            %% Set up any needed latencies
                            ok = setup_latencies(filename:basename(ConfigFile), ClusterMap)
                    end,

                    %% Start things, re-sync NTP
                    ok = sync_nodes(Master, ClusterMap),
                    ok = start_master(Master),
                    ok = start_server(filename:basename(ConfigFile), ClusterMap),

                    %% Actual experiment: load then bench
                    ok = load_ext(Master, ClusterMap),
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
                    ok = stop_server(filename:basename(ConfigFile), ClusterMap),

                    case ConfigFile =:= NextConfig of
                        true ->
                            %% Next experiment will reuse our cluster, no need to clean up
                            ok;
                        false ->
                            %% Clean up after the experiment
                            ok = cleanup_latencies(filename:basename(ConfigFile), ClusterMap),
                            ok = cleanup_master(Master),
                            ok = cleanup_servers(ClusterMap),
                            ok = cleanup_clients(ClusterMap)
                    end,

                    ok
                catch
                    error:Exception:Stack ->
                        %% An exception happened, clean up everything just in case
                        brutal_client_kill(ClusterMap),
                        cleanup_latencies(filename:basename(ConfigFile), ClusterMap),
                        cleanup_master(Master),
                        cleanup_servers(ClusterMap),
                        cleanup_clients(ClusterMap),
                        {error, {Exception, Stack}};

                    throw:Term:Stack ->
                        %% An exception happened, clean up everything just in case
                        brutal_client_kill(ClusterMap),
                        cleanup_latencies(filename:basename(ConfigFile), ClusterMap),
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

preprocess_args(Opts, ConfigFile) ->
    {ok, TokenTerms} = file:consult(?TOKEN_CONFIG),
    {token, Token} = lists:keyfind(token, 1, TokenTerms),
    true = ets:insert(?CONF, {token, Token}),

    {ok, ConfigTerms} = file:consult(ConfigFile),
    {clusters, ClusterMap} = lists:keyfind(clusters, 1, ConfigTerms),

    {ext_tag, ExtTag} = lists:keyfind(ext_tag, 1, ConfigTerms),
    true = ets:insert(?CONF, {ext_tag, ExtTag}),

    {ext_local_port, LocalPort} = lists:keyfind(ext_local_port, 1, ConfigTerms),
    true = ets:insert(?CONF, {ext_local_port, LocalPort}),

    case lists:keyfind(cpu_profile, 1, ConfigTerms) of
        false ->
            ok;
        {cpu_profile, ProfilePath} ->
            true = ets:insert(?CONF, {cpu_profile, ProfilePath})
    end,

    {leader_cluster, LeaderCluster} = lists:keyfind(leader_cluster, 1, ConfigTerms),
    case maps:is_key(LeaderCluster, ClusterMap) of
        false ->
            io:fwrite(standard_error, "Bad cluster map: leader cluster not present ~n", []),
            erlang:throw(bad_master);
        true ->
            true = ets:insert(?CONF, {leader_cluster, LeaderCluster}),
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

    {lasp_bench_rebar_profile, ClientProfile} = lists:keyfind(lasp_bench_rebar_profile, 1, ConfigTerms),
    true = ets:insert(?CONF, {dry_run, maps:get(dry_run, Opts, false)}),
    true = ets:insert(?CONF, {silent, maps:get(verbose, Opts, false)}),
    true = ets:insert(?CONF, {lasp_bench_rebar_profile, ClientProfile}),

    {ClusterMap, Master}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Experiment Steps
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec check_nodes(_, _) -> ok | {error, term()}.
check_nodes(Master, ClusterMap) ->
    io:format("Checking that all nodes are up and on the correct governor mode~n"),

    AllNodes = [Master | all_nodes(ClusterMap)],

    UptimeRes = do_in_nodes_par("uptime", AllNodes, infinity),
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

    % Set all nodes to performance governor status, then verify
    _ = do_in_nodes_par(
        "echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor",
        AllNodes,
        infinity
    ),
    GovernorStatus = do_in_nodes_par(
        "cat /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor",
        AllNodes,
        infinity
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
        lists:zip(AllNodes, GovernorStatus)
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
                transfer_script(Node, "measure_cpu.escript"),
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
    case do_in_nodes_par(master_command("download", AuthToken, GitTag), [Master], ?TIMEOUT) of
        {error, Reason} ->
            {error, Reason};
        Print ->
            io:format("~p~n", [Print]),
            ok
    end.

start_master(Master) ->
    GitTag = ets:lookup_element(?CONF, ext_tag, 2),
    Leader = ets:lookup_element(?CONF, leader_cluster, 2),
    NumReplicas = ets:lookup_element(?CONF, n_replicas, 2),
    NumPartitions = ets:lookup_element(?CONF, n_partitions, 2),
    case
        do_in_nodes_par(
                master_command(
                    "run",
                    atom_to_list(Leader),
                    integer_to_list(NumReplicas),
                    integer_to_list(NumPartitions),
                    GitTag
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
    case do_in_nodes_par(master_command("stop"), [Master], ?TIMEOUT) of
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

stop_server(ConfigFile, ClusterMap) ->
    case do_in_nodes_par(server_command(ConfigFile, "stop"), server_nodes(ClusterMap), ?TIMEOUT) of
        {error, _} ->
            error;
        Res ->
            io:format("~p~n", [Res]),
            ok
    end.

brutal_client_kill(ClusterMap) ->
    _ = do_in_nodes_par("pkill -9 beam", client_nodes(ClusterMap), ?TIMEOUT),
    ok.

download_lasp_bench(ClusterMap) ->
    NodeNames = client_nodes(ClusterMap),
    case do_in_nodes_par(client_command("download"), NodeNames, ?TIMEOUT) of
        {error, DlReason} ->
            {error, DlReason};
        Print0 ->
            io:format("~p~n", [Print0]),
            case do_in_nodes_par(client_command("compile"), NodeNames, ?TIMEOUT) of
                {error, CompReason} ->
                    {error, CompReason};
                Print1 ->
                    io:format("~p~n", [Print1]),
                    ok
            end
    end.

load_ext(Master, ClusterMap) ->
    MasterPort = ets:lookup_element(?CONF, master_port, 2),

    Res0 = pmap(
        fun(Node) ->
            transfer_config(Node, "bench_properties.config")
        end,
        client_nodes(ClusterMap),
        ?TIMEOUT
    ),

    case Res0 of
        {error, _} ->
            error;
        _ ->
            TargetClients =
                maps:fold(
                    fun(Replica, #{clients := C}, Acc) ->
                        [ { Replica, hd(lists:usort(C)) } | Acc ]
                    end,
                    [],
                    ClusterMap
                ),

            Res1 =
                pmap(
                    fun({TargetReplica, ClientNode}) ->
                        Command = client_command(
                            "-y load_ext",
                            atom_to_list(Master),
                            integer_to_list(MasterPort),
                            atom_to_list(TargetReplica),
                            "/home/borja.deregil/bench_properties.config"
                        ),
                        Cmd = io_lib:format(
                            "~s \"~s\" ~s",
                            [?IN_NODES_PATH, Command, atom_to_list(ClientNode)]
                        ),
                        safe_cmd(Cmd)
                    end,
                    TargetClients,
                    ?TIMEOUT
                ),
            case Res1 of
                {error, _} ->
                    error;
                _ ->
                    ok
            end
    end.

bench_ext(Master, RunTerms, ClusterMap) ->
    ok = write_terms(filename:join(?CONFIG_DIR, "run.config"), RunTerms),

    NodeNames = client_nodes(ClusterMap),
    case pmap(fun(Node) -> transfer_config(Node, "run.config") end, NodeNames, ?TIMEOUT) of
        {error, _} ->
            error;
        _ ->
            NodesWithReplicas = [
                {Replica, N} ||
                    {Replica, #{clients := C}} <- maps:to_list(ClusterMap),
                    N <- lists:usort(C)
            ],

            MasterPort = ets:lookup_element(?CONF, master_port, 2),

            %% Set up measurements
            RunsForMinutes = proplists:get_value(duration, RunTerms),

            Token = async_for(
                fun(Node) ->
                    Cmd0 = io_lib:format(
                        "./measure_cpu.escript ~b /home/borja.deregil/~s.cpu",
                        [RunsForMinutes, atom_to_list(Node)]
                    ),
                    Cmd = io_lib:format("~s \"~s\" ~s", [?IN_NODES_PATH, Cmd0, atom_to_list(Node)]),
                    safe_cmd(Cmd)
                end,
                all_nodes(ClusterMap)
            ),

            pmap(
                fun({Replica, Node}) ->
                    Command = client_command(
                        "run",
                        "/home/borja.deregil/run.config",
                        Master,
                        integer_to_list(MasterPort),
                        atom_to_list(Replica)
                    ),
                    Cmd = io_lib:format("~s \"~s\" ~s", [?IN_NODES_PATH, Command, atom_to_list(Node)]),
                    safe_cmd(Cmd)
                end,
                NodesWithReplicas,
                infinity %% Ok to wait for a long time here
            ),

            %% Ensure that measurements have terminated
            case async_for_receive(Token, ?TIMEOUT) of
                {error, timeout} ->
                    {error, timeout};
                _ ->
                    ok
            end
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
    io:format("~p~n", [do_in_nodes_seq("rm -rf sources; mkdir -p sources", [Master])]),
    ok.

cleanup_servers(ClusterMap) ->
    ServerNodes = server_nodes(ClusterMap),
    io:format("~p~n", [do_in_nodes_par("rm -rf sources; mkdir -p sources", ServerNodes, infinity)]),
    ok.

cleanup_clients(ClusterMap) ->
    ClientNodes = client_nodes(ClusterMap),
    io:format("~p~n", [do_in_nodes_par("rm -rf sources; mkdir -p sources", ClientNodes, infinity)]),
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
            (read_distinct_no2pc) -> io_lib:format("read_no2pc~b", [proplists:get_value(readonly_ops, RunTerms)]);
            (update) -> io_lib:format("update_~b", [proplists:get_value(writeonly_ops, RunTerms)]);
            (update_distinct) -> io_lib:format("update_~b", [proplists:get_value(writeonly_ops, RunTerms)]);
            (read_write) ->
                {R,W} = proplists:get_value(mixed_read_write, RunTerms),
                io_lib:format("mixed_~b_~b", [R, W]);
            (ping) -> io_lib:format("ping_~b", [proplists:get_value(readonly_ops, RunTerms)]);
            (ping_read) -> io_lib:format("ping_read_~b", [proplists:get_value(readonly_ops, RunTerms)]);
            (Other) ->
                atom_to_list(Other)
        end,
    OpString =
        lists:foldl(
            fun
                ({Op, _}, "") -> io_lib:format("op_~s", [OpToString(Op)]);
                ({Op, _}, Acc) -> io_lib:format("~s+op_~s", [Acc, OpToString(Op)])
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
    PullClients = fun(Timeout) ->
        pmap(
            fun(Node) ->
                NodeStr = atom_to_list(Node),
                TargetPath = filename:join([?RESULTS_DIR, Path, NodeStr]),

                safe_cmd(io_lib:format("mkdir -p ~s", [TargetPath])),

                %% Compress the results before returning, speeds up transfer
                _ = do_in_nodes_seq(client_command("compress"), [Node]),

                %% Transfer results (-C compresses on flight)
                safe_cmd(io_lib:format(
                    "scp -C -i ~s borja.deregil@~s:/home/borja.deregil/results.tar.gz ~s",
                    [?SSH_PRIV_KEY, NodeStr, TargetPath]
                )),

                safe_cmd(io_lib:format(
                    "scp -i ~s borja.deregil@~s:/home/borja.deregil/~s ~s",
                    [?SSH_PRIV_KEY, NodeStr, ConfigFile, TargetPath]
                )),

                %% Transfer CPU load file
                safe_cmd(io_lib:format(
                    "scp -i ~s borja.deregil@~s:/home/borja.deregil/~s.cpu ~s",
                    [?SSH_PRIV_KEY, NodeStr, NodeStr, TargetPath]
                )),

                %% Rename configuration to cluster.config
                safe_cmd(io_lib:format(
                    "cp ~s ~s/cluster.config",
                    [filename:join(TargetPath, ConfigFile), TargetPath]
                )),

                safe_cmd(io_lib:format(
                    "scp -i ~s borja.deregil@~s:/home/borja.deregil/bench_properties.config ~s",
                    [?SSH_PRIV_KEY, NodeStr, TargetPath]
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
                TargetFile = filename:join([?RESULTS_DIR, Path, io_lib:format("~s.log", [NodeStr])]),

                %% Transfer logs (-C compresses on flight)
                safe_cmd(io_lib:format(
                    "scp -C -i ~s borja.deregil@~s:/home/borja.deregil/~s.log ~s",
                    [?SSH_PRIV_KEY, NodeStr, NodeStr, TargetFile]
                )),

                %% Transfer CPU load file
                safe_cmd(io_lib:format(
                    "scp -i ~s borja.deregil@~s:/home/borja.deregil/~s.cpu ~s",
                    [?SSH_PRIV_KEY, NodeStr, NodeStr, filename:dirname(TargetFile)]
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
%% Util
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

master_command(Command) ->
    io_lib:format("./master.sh ~s", [Command]).

master_command(Command, Arg1, Arg2) ->
    io_lib:format("./master.sh ~s ~s ~s", [Command, Arg1, Arg2]).

master_command(Command, Arg1, Arg2, Arg3, Arg4) ->
    io_lib:format("./master.sh ~s ~s ~s ~s ~s", [Command, Arg1, Arg2, Arg3, Arg4]).

server_command(ConfigFile, Command) ->
    io_lib:format("./server.escript -v -f /home/borja.deregil/~s -c ~s", [ConfigFile, Command]).

server_command(ConfigFile, Command, Arg) ->
    io_lib:format("./server.escript -v -f /home/borja.deregil/~s -c ~s=~s", [
        ConfigFile,
        Command,
        Arg
    ]).

client_command(Command) ->
    Profile = ets:lookup_element(?CONF, lasp_bench_rebar_profile, 2),
    io_lib:format("./bench.sh -b ~s -p ~p ~s", [?LASP_BENCH_BRANCH, Profile, Command]).

client_command(Command, Arg1, Arg2, Arg3, Arg4) ->
    Profile = ets:lookup_element(?CONF, lasp_bench_rebar_profile, 2),
    io_lib:format(
        "./bench.sh -b ~s -p ~p ~s ~s ~s ~s ~s",
        [?LASP_BENCH_BRANCH, Profile, Command, Arg1, Arg2, Arg3, Arg4]
    ).

transfer_script(Node, File) ->
    transfer_from(Node, ?SELF_DIR, File).

transfer_config(Node, File) ->
    transfer_from(Node, ?CONFIG_DIR, File).

transfer_from(Node, Path, File) ->
    Cmd = io_lib:format(
        "scp -i ~s ~s/~s borja.deregil@~s:/home/borja.deregil",
        [?SSH_PRIV_KEY, Path, File, atom_to_list(Node)]
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
    Required = [experiment_definition],
    Valid = lists:all(fun(F) -> maps:is_key(F, Opts) end, Required),
    case Valid of
        false ->
            {error, io_lib:format("Missing required fields: ~p", [Required])};
        true ->
            {ok, Opts}
    end.
