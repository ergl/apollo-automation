#!/usr/bin/env escript

-mode(compile).

-export([main/1]).

-define(SELF_DIR, "/Users/ryan/dev/imdea/code/automation").
-define(SSH_PRIV_KEY, "/Users/ryan/.ssh/imdea_id_ed25519").
-define(RESULTS_DIR, "/Users/ryan/dev/imdea/code/automation/results").
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

-define(CONF, configuration).
-define(CONFIG_FILE, "cluster_definition.config").
-define(LOAD_SPEC, #{key_limit => 1000000, val_size => 256}).

% 10 minute timeout for pmap
-define(TIMEOUT, timer:minutes(10)).

-define(COMMANDS, [
    {reboot, false}
    , {check, false}
    , {push_scripts, false}
    , {sync, false}

    , {master, false}
    , {servers, false}
    , {clients, false}
    % Downloads all and starts master, server
    , {prologue, false}

    , {start_master, false}
    , {stop_master, false}
    , {start_servers, false}
    , {stop_servers, false}
    , {start, false}
    , {stop, false}

    , {load, false}
    , {bench, false}
    , {bench_no_load, false}
    , {print_bench_command, false}
    , {brutal_client_kill, false}

    , {restart, false}
    , {cleanup, false}
    , {cleanup_master, false}
    , {cleanup_servers, false}
    , {cleanup_clients, false}
    , {pull, {true, "path"}}
]).

usage() ->
    Name = filename:basename(escript:script_name()),
    Commands = lists:foldl(
        fun({Command, NeedsArg}, Acc) ->
            CommandStr =
                case NeedsArg of
                    true -> io_lib:format("~s=arg", [Command]);
                    {true, ArgName} -> io_lib:format("~s=[~s]", [Command, ArgName]);
                    false -> io_lib:format("~s", [Command])
                end,
            case Acc of
                "" -> io_lib:format("~s", [CommandStr]);
                _ -> io_lib:format("~s | ~s", [Acc, CommandStr])
            end
        end,
        "",
        ?COMMANDS
    ),
    ok = io:fwrite(
        standard_error,
        "Usage: ~s --cluster_config <config-file> --run_config <run-config> -c <command=arg>~nCommands: < ~s >~n",
        [Name, Commands]
    ).

main(Args) ->
    case parse_args(Args) of
        {error, Reason} ->
            io:fwrite(standard_error, "Wrong option: reason ~s~n", [Reason]),
            usage(),
            halt(1);

        {ok, Opts = #{ config := Config, command := Command, run_config := RunConfig }} ->
            {ok, ConfigTerms} = file:consult(Config),
            {ok, RunTerms} = file:consult(RunConfig),
            _ = ets:new(?CONF, [set, named_table]),

            case catch preprocess_args(Opts, ConfigTerms, RunTerms) of
                {'EXIT', TraceBack} ->
                    ets:delete(?CONF),
                    io:fwrite(standard_error, "Preprocess error: ~p~n", [TraceBack]);

                {ClusterMap, Latencies, Master} ->
                    case maps:get(command_arg, Opts, false) of
                        false ->
                            case Command of
                                bench ->
                                    ok = do_command({Command, RunConfig}, Master, ClusterMap, Latencies);
                                bench_lo_load ->
                                    ok = do_command({Command, RunConfig}, Master, ClusterMap, Latencies);
                                print_bench_command ->
                                    ok = do_command({Command, RunConfig}, Master, ClusterMap, Latencies);
                                _ ->
                                    ok = do_command(Command, Master, ClusterMap, Latencies)
                            end;
                        CommandArg ->
                            ok = do_command({Command, CommandArg}, Master, ClusterMap, Latencies)
                    end, 
                    true = ets:delete(?CONF),
                    ok
            end
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

    {leader_preference, PrefList} = lists:keyfind(leader_preference, 1, ConfigTerms),
    true = ets:insert(?CONF, {leader_preference, PrefList}),

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

    {latencies, Latencies} = lists:keyfind(latencies, 1, ConfigTerms),

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

    {ClusterMap, Latencies, Master}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Commands
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

do_command(reboot, Master, ClusterMap, _) ->
    prompt_gate("Are you sure you want to reboot?", default_no, fun() ->
        AllNodes = all_nodes(ClusterMap),
        io:format("~p~n", [do_in_nodes_par("sudo reboot", [Master | AllNodes], ?TIMEOUT)])
    end);

do_command(check, Master, ClusterMap, _) ->
    check_nodes(Master, ClusterMap);

do_command(push_scripts, Master, ClusterMap, _) ->
    push_scripts(?CONFIG_FILE, Master, ClusterMap);

do_command(sync, Master, ClusterMap, _) ->
    sync_nodes(Master, ClusterMap);

do_command(master, Master, _ClusterMap, LatencyMap) ->
    ok = download_master(Master),
    ok = start_master(Master, LatencyMap);

do_command(servers, _Master, ClusterMap, _) ->
    ok = download_server(?CONFIG_FILE, ClusterMap),
    ok = start_server(?CONFIG_FILE, ClusterMap);

do_command(clients, _Master, ClusterMap, _) ->
    ok = download_runner(ClusterMap);

do_command(prologue, Master, ClusterMap, LatencyMap) ->
    ok = do_command(check, Master, ClusterMap, LatencyMap),
    ok = do_command(push_scripts, Master, ClusterMap, LatencyMap),
    ok = do_command(sync, Master, ClusterMap, LatencyMap),
    ok = do_command(master, Master, ClusterMap, LatencyMap),
    ok = do_command(servers, Master, ClusterMap, LatencyMap),
    ok = do_command(clients, Master, ClusterMap, LatencyMap);

do_command(start_master, Master, _, LatencyMap) ->
    ok = start_master(Master, LatencyMap);

do_command(stop_master, Master, _, _) ->
    ok = stop_master(Master);

do_command(start_servers, _Master, ClusterMap, _) ->
    ok = start_server(?CONFIG_FILE, ClusterMap);

do_command(stop_servers, _Master, ClusterMap, _) ->
    ok = stop_server(?CONFIG_FILE, ClusterMap);

do_command(start, Master, ClusterMap, LatencyMap) ->
    ok = do_command(start_master, Master, ClusterMap, LatencyMap),
    ok = do_command(start_servers, Master, ClusterMap, LatencyMap);

do_command(stop, Master, ClusterMap, LatencyMap) ->
    ok = do_command(stop_master, Master, ClusterMap, LatencyMap),
    ok = do_command(stop_servers, Master, ClusterMap, LatencyMap);

do_command(load, Master, ClusterMap, _) ->
    ok = load_ext(Master, ClusterMap, ?LOAD_SPEC);

do_command({bench, RunConfigFileName}, Master, ClusterMap, LatencyMap) ->
    ok = do_command(load, Master, ClusterMap, LatencyMap),
    ok = do_command({bench_no_load, RunConfigFileName}, Master, ClusterMap, LatencyMap);

do_command({bench_no_load, RunConfigFileName}, Master, ClusterMap, _) ->
    {ok, RunTerms} = file:consult(RunConfigFileName),
    ok = bench_ext(Master, RunTerms, ClusterMap);

do_command({print_bench_command, RunConfigFileName}, Master, ClusterMap, _) ->
    {ok, RunTerms} = file:consult(RunConfigFileName),
    ok = print_bench_command(Master, RunTerms, ClusterMap);

do_command(brutal_client_kill, _Master, ClusterMap, _) ->
    ok = brutal_client_kill(ClusterMap);

do_command(restart, Master, ClusterMap, LatencyMap) ->
    ok = do_command(stop, Master, ClusterMap, LatencyMap),
    ok = do_command(start, Master, ClusterMap, LatencyMap);

do_command(cleanup_master, Master, _ClusterMap, _) ->
    ok = cleanup_master(Master);

do_command(cleanup_servers, _Master, ClusterMap, _) ->
    ok = cleanup_servers(ClusterMap);

do_command(cleanup_clients, _Master, ClusterMap, _) ->
    ok = cleanup_clients(ClusterMap);

do_command(cleanup, Master, ClusterMap, LatencyMap) ->
    prompt_gate("Are you sure you want to clean up?", default_no, fun() ->
        ok = do_command(cleanup_master, Master, ClusterMap, LatencyMap),
        ok = do_command(cleanup_servers, Master, ClusterMap, LatencyMap),
        ok = do_command(cleanup_clients, Master, ClusterMap, LatencyMap)
    end);

do_command({pull, Directory}, _Master, ClusterMap, _) ->
    ok = pull_results(ets:lookup_element(?CONF, client_variant, 2), ?CONFIG_FILE, Directory, ClusterMap).

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

start_master(Master, LatencyMap) ->
    GitTag = ets:lookup_element(?CONF, ext_tag, 2),
    NumReplicas = ets:lookup_element(?CONF, n_replicas, 2),
    NumPartitions = ets:lookup_element(?CONF, n_partitions, 2),
    LeaderPreferences = ets:lookup_element(?CONF, leader_preference, 2),

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

    % FIXME(borja): Add a configuration key for leader order
    ArgString1 =
        lists:foldl(
            fun(Replica, Acc) ->
                io_lib:format("~s -leaderChoice ~s", [Acc, Replica])
            end,
            ArgString0,
            LeaderPreferences
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
            LatencyMap
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

bench_ext(Master, RunTerms, ClusterMap) ->
    bench_ext(ets:lookup_element(?CONF, client_variant, 2), Master, RunTerms, ClusterMap).

bench_ext(go_runner, Master, RunTerms, ClusterMap) ->
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
                        io_lib:format("~s -distribution uniform", [Acc]);
                    {key_distribution, pareto} ->
                        io_lib:format("~s -distribution pareto", [Acc]);
                    {key_distribution, split_uniform} ->
                        io_lib:format("~s -distribution split_uniform", [Acc]);
                    {key_distribution, {biased_key, Key, Bias}} when is_integer(Key) andalso is_integer(Bias) ->
                        io_lib:format("~s -distribution biased_key -distrArgs '-hotKey ~b -bias ~b'", [Acc, Key, Bias]);
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
    end;

bench_ext(lasp_bench_runner, Master, RunTerms, ClusterMap) ->
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

print_bench_command(Master, RunTerms, ClusterMap) ->
    print_bench_command(ets:lookup_element(?CONF, client_variant, 2), Master, RunTerms, ClusterMap).

print_bench_command(go_runner, Master, RunTerms, ClusterMap) ->
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
                        io_lib:format("~s -distribution uniform", [Acc]);
                    {key_distribution, pareto} ->
                        io_lib:format("~s -distribution pareto", [Acc]);
                    {key_distribution, split_uniform} ->
                        io_lib:format("~s -distribution split_uniform", [Acc]);
                    {key_distribution, {biased_key, Key, Bias}} when is_integer(Key) andalso is_integer(Bias) ->
                        io_lib:format("~s -distribution biased_key -distrArgs \"-hotKey ~b -bias ~b\"", [Acc, Key, Bias]);
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
    lists:foreach(
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

            io:format("~s ~s~n", [Command, NodeStr])
        end,
        NodesWithReplicas
    ),
    ok;

print_bench_command(lasp_bench_runner, Master, _RunTerms, ClusterMap) ->
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
        lists:foreach(
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

                io:format("~s ~s~n", [Command, NodeStr])
            end,
            NodesWithReplicas
        ),
        ok
    end.

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

pull_results(go_runner, ConfigFile, Path, ClusterMap) ->
    GitTag = ets:lookup_element(?CONF, ext_tag, 2),
    PullClients = fun(Timeout) ->
        pmap(
            fun(Node) ->
                NodeStr = atom_to_list(Node),
                HomePathForNode = home_path_for_node(NodeStr),
                ResultPath = io_lib:format("~s/runner_results/current", [home_path_for_node(NodeStr)]),
                TargetPath = filename:join([Path, NodeStr]),

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
                % Prefix folder with '_' to be excluded later
                TargetPath = filename:join([Path, io_lib:format("_~s", [NodeStr])]),

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

                %% Transfer pcap if it exists
                safe_cmd(io_lib:format(
                    "scp -C -i ~s borja.deregil@~s:~s/server.pcap ~s",
                    [?SSH_PRIV_KEY, NodeStr, HomePathForNode, TargetPath]
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
            ok
    end;

pull_results(lasp_bench_runner, ConfigFile, Path, ClusterMap) ->
    PullClients = fun(Timeout) ->
        pmap(
            fun(Node) ->
                NodeStr = atom_to_list(Node),
                HomePathForNode = home_path_for_node(NodeStr),
                TargetPath = filename:join([Path, NodeStr]),

                safe_cmd(io_lib:format("mkdir -p ~s", [TargetPath])),

                %% Compress the results before returning, speeds up transfer
                _ = do_in_nodes_seq(erlang_client_command(NodeStr, "compress"), [Node]),

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
                TargetPath = filename:join([Path, io_lib:format("_~s", [NodeStr])]),

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

                %% Transfer pcap if it exists
                safe_cmd(io_lib:format(
                    "scp -C -i ~s borja.deregil@~s:~s/server.pcap ~s",
                    [?SSH_PRIV_KEY, NodeStr, HomePathForNode, TargetPath]
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
            ok
    end.

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

client_nodes(Map) ->
    lists:usort(lists:flatten([N || #{clients := N} <- maps:values(Map)])).

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

prompt_gate(Msg, Default, Fun) ->
    case prompt(Msg, Default) of
        true ->
            Fun(),
            ok;
        false ->
            io:format("Cancelling~n"),
            ok
    end.

prompt(Msg, Default) ->
    {Prompt, Validate} = case Default of
        default_no ->
            {
                Msg ++ " [N/y]: ",
                fun(I) when I =:= "y\n" orelse I =:= "Y\n" -> true; (_) -> false end
            };

        default_yes ->
            {
                Msg ++ " [Y/n]: ",
                fun(I) when I =:= "n\n" orelse I =:= "N\n" -> true; (_) -> false end
            }
    end,
    Validate(io:get_line(Prompt)).

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
        [$f] ->
            parse_flag(Flag, Args, fun(Arg) -> Acc#{config => Arg} end);
        "-cluster_config" ->
            parse_flag(Flag, Args, fun(Arg) -> Acc#{config => Arg} end);
        [$r] ->
            parse_flag(Flag, Args, fun(Arg) -> Acc#{run_config => Arg} end);
        "-run_config" ->
            parse_flag(Flag, Args, fun(Arg) -> Acc#{run_config => Arg} end);
        [$c] ->
            parse_flag(Flag, Args, fun(Arg) -> parse_command(Arg, Acc) end);
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

parse_command(Arg, Acc) ->
    case string:str(Arg, "=") of
        0 ->
            Acc#{command => list_to_atom(Arg)};
        _ ->
            % crash on malformed command for now
            [Command, CommandArg | _Ignore] = string:tokens(Arg, "="),
            Acc#{command_arg => CommandArg, command => list_to_atom(Command)}
    end.

required(Opts) ->
    Required = [config, command, run_config],
    Valid = lists:all(fun(F) -> maps:is_key(F, Opts) end, Required),
    case Valid of
        false ->
            {error, io_lib:format("Missing required fields: ~p", [Required])};
        true ->
            case maps:is_key(command, Opts) of
                true -> check_command(Opts);
                false -> {ok, Opts}
            end
    end.

check_command(Opts = #{command := Command}) ->
    case lists:keyfind(Command, 1, ?COMMANDS) of
        {Command, true} when is_map_key(command_arg, Opts) ->
            {ok, Opts};
        {Command, {true, _}} when is_map_key(command_arg, Opts) ->
            {ok, Opts};
        {Command, false} ->
            {ok, Opts};
        _ ->
            {error,
                io_lib:format("Bad command \"~p\", or command needs arg, but none was given", [
                    Command
                ])}
    end.
