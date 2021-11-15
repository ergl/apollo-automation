#!/usr/bin/env escript

%% TODO(borja): Measure CPU utilization at the server
%% We can use mpstat like: `mpstat 2 1 | awk 'END{print 100-$NF"%"}'`
%% prints the CPU utilization over the last two seconds, and does 100 - idle
%% we could spawn a separate process to a server and measure CPU during the
%% benchmark, then return that at the end, and use that as a way to see if we
%% should continue running benchmarks.

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

run_experiments(_, []) ->
    ok;
run_experiments(Opts, [ Spec | Rest ]) ->
    case execute_spec(Opts, Spec) of
        ok ->
            run_experiments(Opts, Rest);
        {error, Reason} ->
            io:fwrite(standard_error, "Spec error on ~p: ~p~n", [Spec, Reason]),
            error
    end.

execute_spec(Opts, #{config := ConfigFile, results_folder := Results, run_terms := RunTerms}) ->
    _ = ets:new(?CONF, [set, named_table]),
    Result =
        try
            {ClusterMap, Master} = preprocess_args(Opts, ConfigFile),

            %% First, sanity check, send all necessary scripts to all nodes
            ok = check_nodes(Master, ClusterMap),
            ok = push_scripts(Master, ClusterMap),
            ok = sync_nodes(Master, ClusterMap),
            ok = prepare_master(Master),
            ok = prepare_server(ClusterMap),
            ok = prepare_lasp_bench(ClusterMap),

            %% Set up any needed latencies
            ok = setup_latencies(ClusterMap),

            %% Actual experiment: load then bench
            ok = load_ext(Master, ClusterMap),
            ok = bench_ext(Master, RunTerms, ClusterMap),

            %% Give system some time (1 sec) to stabilise
            ok = timer:sleep(1000),

            %% Gather all results from the experiment
            ok = pull_results(Results, RunTerms, ClusterMap),

            %% Stop all nodes
            ok = stop_master(Master),
            ok = stop_server(ClusterMap),

            %% Clean up after the experiment
            ok = cleanup_latencies(ClusterMap),
            ok = cleanup_master(Master),
            ok = cleanup_servers(ClusterMap),
            ok = cleanup_clients(ClusterMap),

            ok
        catch
            throw:Term ->
                {error, Term}
        end,
    ets:delete(?CONF),
    Result.

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

check_nodes(Master, ClusterMap) ->
    io:format("Checking that all nodes are up and on the correct governor mode~n"),

    AllNodes = [Master | all_nodes(ClusterMap)],

    UptimeRes = do_in_nodes_par("uptime", AllNodes),
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
        AllNodes
    ),
    GovernorStatus = do_in_nodes_par(
        "cat /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor",
        AllNodes
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

push_scripts(Master, ClusterMap) ->
    % Transfer server, bench and cluster config
    AllNodes = [Master | all_nodes(ClusterMap)],
    io:format("Transfering benchmark config files (server, bench, cluster)...~n"),
    pmap(
        fun(Node) ->
            transfer_script(Node, "master.sh"),
            transfer_script(Node, "server.escript"),
            transfer_script(Node, "bench.sh"),
            transfer_script(Node, "build_tc_rules.escript"),
            transfer_script(Node, "my_ip"),
            transfer_script(Node, "fetch_gh_release.sh"),
            transfer_config(Node, "cluster.config")
        end,
        AllNodes
    ),
    ok.

sync_nodes(Master, ClusterMap) ->
    io:format("Resyncing NTP on all nodes~n"),
    AllNodes = [Master | all_nodes(ClusterMap)],
    _ = do_in_nodes_par("sudo service ntp stop", AllNodes),
    _ = do_in_nodes_par("sudo ntpd -gq system.imdea", AllNodes),
    _ = do_in_nodes_par("sudo service ntp start", AllNodes),
    ok.

prepare_master(Master) ->
    ok = download_master(Master),
    ok = start_master(Master).

download_master(Master) ->
    AuthToken = ets:lookup_element(?CONF, token, 2),
    GitTag = ets:lookup_element(?CONF, ext_tag, 2),
    io:format("~p~n", [do_in_nodes_seq(master_command("download", AuthToken, GitTag), [Master])]),
    ok.

start_master(Master) ->
    GitTag = ets:lookup_element(?CONF, ext_tag, 2),
    Leader = ets:lookup_element(?CONF, leader_cluster, 2),
    NumReplicas = ets:lookup_element(?CONF, n_replicas, 2),
    NumPartitions = ets:lookup_element(?CONF, n_partitions, 2),
    io:format(
        "~p~n",
        [
            do_in_nodes_par(
                master_command(
                    "run",
                    atom_to_list(Leader),
                    integer_to_list(NumReplicas),
                    integer_to_list(NumPartitions),
                    GitTag
                ),
                [Master]
            )
        ]
    ),
    ok.

stop_master(Master) ->
    io:format("~p~n", [do_in_nodes_seq(master_command("stop"), [Master])]),
    ok.

prepare_server(ClusterMap) ->
    ok = download_server(ClusterMap),
    ok = start_server(ClusterMap).

download_server(ClusterMap) ->
    AuthToken = ets:lookup_element(?CONF, token, 2),
    io:format("~p~n", [do_in_nodes_par(server_command("download", AuthToken), server_nodes(ClusterMap))]),
    ok.

start_server(ClusterMap) ->
    ok = maps:fold(
        fun(ClusterName, #{servers := ServerNodes}, _) ->
            Res = do_in_nodes_par(server_command("start", atom_to_list(ClusterName)), lists:usort(ServerNodes)),
            io:format("~p: ~p~n", [ClusterName, Res]),
            ok
        end,
        ok,
        ClusterMap
    ).

stop_server(ClusterMap) ->
    io:format("~p~n", [do_in_nodes_par(server_command("stop"), server_nodes(ClusterMap))]),
    ok.

prepare_lasp_bench(ClusterMap) ->
    NodeNames = client_nodes(ClusterMap),
    io:format("~p~n", [do_in_nodes_par(client_command("download"), NodeNames)]),
    _ = do_in_nodes_par(client_command("compile"), NodeNames),
    ok.

load_ext(Master, ClusterMap) ->
    MasterPort = ets:lookup_element(?CONF, master_port, 2),

    pmap(
        fun(Node) ->
            transfer_config(Node, "bench_properties.config")
        end,
        client_nodes(ClusterMap)
    ),

    TargetClients =
        maps:fold(
            fun(Replica, #{clients := C}, Acc) ->
                [ { Replica, hd(lists:usort(C)) } | Acc ]
            end,
            [],
            ClusterMap
        ),
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
        TargetClients
    ),
    ok.

bench_ext(Master, RunTerms, ClusterMap) ->
    ok = write_terms(filename:join(?CONFIG_DIR, "run.config"), RunTerms),

    NodeNames = client_nodes(ClusterMap),
    pmap(fun(Node) -> transfer_config(Node, "run.config") end, NodeNames),

    NodesWithReplicas = [
        {Replica, N} ||
            {Replica, #{clients := C}} <- maps:to_list(ClusterMap),
            N <- lists:usort(C)
    ],

    MasterPort = ets:lookup_element(?CONF, master_port, 2),

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
        NodesWithReplicas
    ),
    ok.

-spec setup_latencies(_) -> ok.
setup_latencies(ClusterMap) ->
    maps:fold(
        fun(ClusterName, #{servers := ClusterServers}, _Acc) ->
            io:format(
                "~p~n",
                [
                    do_in_nodes_par(
                        server_command("tc", atom_to_list(ClusterName)),
                        ClusterServers
                    )
                ]
            ),
            ok
        end,
        ok,
        ClusterMap
    ).

cleanup_latencies(ClusterMap) ->
    maps:fold(
        fun(ClusterName, #{servers := ClusterServers}, _Acc) ->
            io:format(
                "~p~n",
                [
                    do_in_nodes_par(
                        server_command("tclean", atom_to_list(ClusterName)),
                        ClusterServers
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
    io:format("~p~n", [do_in_nodes_par("rm -rf sources; mkdir -p sources", ServerNodes)]),
    ok.

cleanup_clients(ClusterMap) ->
    ClientNodes = client_nodes(ClusterMap),
    io:format("~p~n", [do_in_nodes_par("rm -rf sources; mkdir -p sources", ClientNodes)]),
    ok.

pull_results(ResultsFolder, RunTerms, ClusterMap) ->
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
            (update) -> io_lib:format("update_~b", [proplists:get_value(writeonly_ops, RunTerms)]);
            (update_distinct) -> io_lib:format("update_~b", [proplists:get_value(writeonly_ops, RunTerms)]);
            (read_write) ->
                {R,W} = proplists:get_value(mixed_read_write, RunTerms),
                io_lib:format("mixed_~b_~b", [R, W]);
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
        "~s_partitions_~b+cl_~b+cm_~b+~s+t_~b",
        [
            calendar:system_time_to_rfc3339(erlang:system_time(millisecond), [{unit, millisecond}]),
            maps:size(ClusterMap),
            NPartitions,
            NClients,
            case OpString of "" -> "op_NA"; _ -> OpString end,
            proplists:get_value(concurrent, RunTerms, "NA")
        ]
    ),
    pull_results_to_path(ClusterMap, filename:join(ResultsFolder, Path)).

pull_results_to_path(ClusterMap, Path) ->
    PullClients = fun() ->
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
                    "scp -i ~s borja.deregil@~s:/home/borja.deregil/cluster.config ~s",
                    [?SSH_PRIV_KEY, NodeStr, TargetPath]
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
            client_nodes(ClusterMap)
        )
    end,

    PullServerLogs = fun() ->
        pmap(
            fun(Node) ->
                NodeStr = atom_to_list(Node),
                TargetFile = filename:join([?RESULTS_DIR, Path, io_lib:format("~s.log", [NodeStr])]),

                %% Transfer logs (-C compresses on flight)
                safe_cmd(io_lib:format(
                    "scp -C -i ~s borja.deregil@~s:/home/borja.deregil/~s.log ~s",
                    [?SSH_PRIV_KEY, NodeStr, NodeStr, TargetFile]
                )),

                ok
            end,
            server_nodes(ClusterMap)
        )
    end,

    DoFun = fun() ->
        _ = PullClients(),
        _ = PullServerLogs(),
        ok
    end,

    DoFun(),

    %% Compress everything into a single archive file
    ResultsPath = filename:join(?RESULTS_DIR, Path),
    safe_cmd(io_lib:format(
        "tar -czf ~s.tar.gz ~s",
        [ResultsPath, filename:basename(ResultsPath)]
    )),

    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Util
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

master_command(Command) ->
    io_lib:format("./master.sh ~s", [Command]).

master_command(Command, Arg1, Arg2) ->
    io_lib:format("./master.sh ~s ~s ~s", [Command, Arg1, Arg2]).

master_command(Command, Arg1, Arg2, Arg3, Arg4) ->
    io_lib:format("./master.sh ~s ~s ~s ~s ~s", [Command, Arg1, Arg2, Arg3, Arg4]).

server_command(Command) ->
    io_lib:format("./server.escript -v -f /home/borja.deregil/cluster.config -c ~s", [Command]).

server_command(Command, Arg) ->
    io_lib:format("./server.escript -v -f /home/borja.deregil/cluster.config -c ~s=~s", [
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

do_in_nodes_par(Command, Nodes) ->
    pmap(
        fun(Node) ->
            Cmd = io_lib:format("~s \"~s\" ~s", [?IN_NODES_PATH, Command, atom_to_list(Node)]),
            safe_cmd(Cmd)
        end,
        Nodes
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

pmap(F, L) ->
    Parent = self(),
    lists:foldl(
        fun(X, N) ->
            spawn_link(fun() -> Parent ! {pmap, N, F(X)} end),
            N + 1
        end,
        0,
        L
    ),
    L2 = [
        receive
            {pmap, N, R} -> {N, R}
        end
        || _ <- L
    ],
    L3 = lists:keysort(1, L2),
    [R || {_, R} <- L3].

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
