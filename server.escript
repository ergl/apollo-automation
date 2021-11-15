#!/usr/bin/env escript

-mode(compile).

-export([main/1]).

-define(DEFAULT_LOG_PATH, "/home/borja.deregil").
-define(DEFAULT_LOG_LEVEL, 2).
-define(DEFAULT_BIN_NAME, "server_linux_amd64").
-define(DEFAULT_LOG_SIZE, 25).
-define(DEFAULT_PING_INTERVAL, 100).
-define(DEFAULT_FAULT_TOLERANCE_FACTOR, 1).
-define(DEFAULT_LISTEN_PORT, 7070).
-define(DEFAULT_INTER_DC_PORT, 8989).
-define(DEFAULT_MASTER_PORT, 7087).
-define(COMMANDS, [
    {download, {true, "Github Token"}},
    {start, {true, "Replica Name"}},
    {profile, false},
    {stop, false},
    {restart, {true, "Replica Name"}},
    {tc, {true, "Replica Name"}},
    {tclean, {true, "Replica Name"}}
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
                "" -> io_lib:format("< ~s", [CommandStr]);
                _ -> io_lib:format("~s | ~s", [Acc, CommandStr])
            end
        end,
        "",
        ?COMMANDS
    ),
    ok = io:fwrite(
        standard_error,
        "Usage: [-dv] ~s -f <config-file> -c ~s~n",
        [Name, Commands ++ " >"]
    ).

main(Args) ->
    case parse_args(Args) of
        {error, Reason} ->
            io:fwrite(standard_error, "Wrong option: reason ~s~n", [Reason]),
            usage(),
            halt(1);
        {ok, Parsed = #{config := ConfigFile, command := Command}} ->
            erlang:put(dry_run, maps:get(dry_run, Parsed, false)),
            erlang:put(verbose, maps:get(verbose, Parsed, false)),

            {ok, Config} = file:consult(ConfigFile),
            case maps:get(command_arg, Parsed, undefined) of
                undefined -> execute_command(Command, Config);
                Arg -> execute_command({Command, Arg}, Config)
            end
    end.

execute_command({download, Token}, Config) ->
    {ok, Tag} = get_config_key(ext_tag, Config),
    Folder = io_lib:format("sources/~s", [Tag]),
    Cmd0 =
        io_lib:format(
            "GITHUB_API_TOKEN=~s ./fetch_gh_release.sh -t ~s -f ~s",
            [Token, Tag, ?DEFAULT_BIN_NAME]
        ),
    os_cmd(Cmd0),
    Cmd1 = io_lib:format("chmod u+x ~s", [?DEFAULT_BIN_NAME]),
    os_cmd(Cmd1),
    Cmd2 = io_lib:format("mkdir -p ~s", [Folder]),
    os_cmd(Cmd2),
    Cmd3 = io_lib:format("mv ~s ~s", [?DEFAULT_BIN_NAME, Folder]),
    os_cmd(Cmd3),
    ok;

execute_command({start, Replica}, Config) ->
    ok = start_ext(Replica, Config);

execute_command(profile, Config) ->
    ok = dump_profile(Config);

execute_command(stop, Config) ->
    ok = stop_ext(Config);

execute_command({restart, Replica}, Config) ->
    ok = stop_ext(Config),
    ok = start_ext(Replica, Config),
    ok;

execute_command({tc, ClusterName}, _) ->
    Cmd = io_lib:format(
        "escript -c -n build_tc_rules.escript -c ~s -f /home/borja.deregil/cluster.config -r run",
        [ClusterName]
    ),
    os_cmd(Cmd),
    ok;

execute_command({tclean, ClusterName}, _) ->
    Cmd = io_lib:format(
        "escript -c -n build_tc_rules.escript -c ~s -f /home/borja.deregil/cluster.config -r cleanup",
        [ClusterName]
    ),
    os_cmd(Cmd),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_ext(Replica, Config) ->
    _ = os_cmd("sudo sysctl net.ipv4.ip_local_port_range=\"15000 61000\""),

    {ok, MASTER_NODE} = get_config_key(master_node, Config),
    MASTER_PORT = get_config_key(master_port, Config, ?DEFAULT_MASTER_PORT),

    IP = get_current_ip_addres(),
    PORT = get_config_key(ext_local_port, Config, ?DEFAULT_LISTEN_PORT),
    INTER_DC_PORT = get_config_key(ext_dc_port, Config, ?DEFAULT_INTER_DC_PORT),

    VSN_LOG_SIZE = get_config_key(version_log_size, Config, ?DEFAULT_LOG_SIZE),

    PING_INTERVAL_MS = get_config_key(
        ping_interval_ms,
        Config,
        ?DEFAULT_PING_INTERVAL
    ),

    FAULT_TOLERANCE_FACTOR = get_config_key(
        fault_tolerance_factor,
        Config,
        ?DEFAULT_FAULT_TOLERANCE_FACTOR
    ),

    LogPath = get_config_key(log_path, Config, ?DEFAULT_LOG_PATH),
    LOG_LEVEL = get_config_key(log_level, Config, ?DEFAULT_LOG_LEVEL),
    LOG_FILE = get_log_file(LogPath),

    ArgString0 = io_lib:format(
        "-replica ~s -ip ~s -port ~b -replPort ~b -mIp ~s -mPort ~b -versions ~b -pingMs ~b -f ~b -log ~s -log_level ~b",
        [
            Replica,
            IP,
            PORT,
            INTER_DC_PORT,
            MASTER_NODE,
            MASTER_PORT,
            VSN_LOG_SIZE,
            PING_INTERVAL_MS,
            FAULT_TOLERANCE_FACTOR,
            LOG_FILE,
            LOG_LEVEL
        ]
    ),

    ArgString1 =
        case get_config_key(cpu_profile, Config) of
            {ok, FilePath} ->
                ArgString0 ++ io_lib:format(" -cpuprofile ~s", [FilePath]);
            error ->
                ArgString0
        end,

    ArgString2 =
        case get_config_key(inter_dc_pool_size, Config) of
            {ok, DCSize} ->
                ArgString1 ++ io_lib:format(" -dc_pool ~b", [DCSize]);
            error ->
                ArgString1
        end,

    ArgString3 =
        case get_config_key(local_dc_pool_size, Config) of
            {ok, LocalSize} ->
                ArgString2 ++ io_lib:format(" -local_pool ~b", [LocalSize]);
            error ->
                ArgString2
        end,

    {ok, Tag} = get_config_key(ext_tag, Config),
    Cmd = io_lib:format(
        "screen -dmSL ~s ./sources/~s/~s ~s",
        [?DEFAULT_BIN_NAME, Tag, ?DEFAULT_BIN_NAME, ArgString3]
    ),

    os_cmd(Cmd),

    ok.

stop_ext(Config) ->
    ok = case get_config_key(cpu_profile, Config) of
        error ->
            stop_ext_normal();
        {ok, _} ->
            stop_ext_profile(get_config_key(ext_dc_port, Config, ?DEFAULT_INTER_DC_PORT))
    end.

stop_ext_normal() ->
    Cmd = io_lib:format("screen -ls | grep -o -P \"\\d+.~s\"", [?DEFAULT_BIN_NAME]),
    ScreenName = nonl(os_cmd_ignore_verbose(Cmd)),
    case ScreenName of
        "" ->
            io:format("Couldn't find server process\n"),
            ok;
        _ ->
            Cmd1 = io_lib:format("screen -X -S ~s quit", [ScreenName]),
            os_cmd(Cmd1),
            ok
    end.

stop_ext_profile(ListenPort) ->
    Cmd0 = io_lib:format("lsof -nP -iTCP:~b -t", [ListenPort]),
    Pid = nonl(os_cmd_ignore_verbose(Cmd0)),
    true = Pid =/= [],

    % This also terminates any screen sessions
    Cmd1 = io_lib:format("kill -9 ~s", [Pid]),
    os_cmd(Cmd1),

    % Sanity check to ensure that screen terminated
    Cmd2 = io_lib:format("screen -ls | grep -o -P \"\\d+.~s\"", [?DEFAULT_BIN_NAME]),
    [] = os_cmd_ignore_verbose(Cmd2),

    ok.

dump_profile(Config) ->
    ListenPort = get_config_key(ext_dc_port, Config, ?DEFAULT_INTER_DC_PORT),
    Cmd0 = io_lib:format("lsof -nP -iTCP:~b -t", [ListenPort]),
    Pid = nonl(os_cmd_ignore_verbose(Cmd0)),
    true = Pid =/= [],

    Cmd1 = io_lib:format("kill -s USR1 ~s", [Pid]),
    os_cmd(Cmd1),

    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% help
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec os_cmd(string()) -> ok.
os_cmd(Cmd) ->
    Verbose = erlang:get(verbose),
    DryRun = erlang:get(dry_run),
    case Verbose of
        true -> io:format("$ ~s~n", [Cmd]);
        false -> ok
    end,
    case DryRun of
        true ->
            ok;
        false ->
            Ret = os:cmd(Cmd),
            case Verbose of
                true -> io:format("~s~n", [Ret]);
                false -> ok
            end
    end.

os_cmd_ignore_verbose(Cmd) ->
    case erlang:get(dry_run) of
        true -> "";
        false -> os:cmd(Cmd)
    end.

-spec get_current_ip_addres() -> string().
get_current_ip_addres() ->
    {ok, Hostname} = inet:gethostname(),
    ip_for_node(Hostname).

ip_for_node("apollo-1-1") -> "10.10.5.31";
ip_for_node("apollo-1-2") -> "10.10.5.32";
ip_for_node("apollo-1-3") -> "10.10.5.33";
ip_for_node("apollo-1-4") -> "10.10.5.34";
ip_for_node("apollo-1-5") -> "10.10.5.35";
ip_for_node("apollo-1-6") -> "10.10.5.36";
ip_for_node("apollo-1-7") -> "10.10.5.37";
ip_for_node("apollo-1-8") -> "10.10.5.38";
ip_for_node("apollo-1-9") -> "10.10.5.39";
ip_for_node("apollo-1-10") -> "10.10.5.40";
ip_for_node("apollo-1-11") -> "10.10.5.41";
ip_for_node("apollo-1-12") -> "10.10.5.42";
ip_for_node("apollo-2-1") -> "10.10.5.61";
ip_for_node("apollo-2-2") -> "10.10.5.62";
ip_for_node("apollo-2-3") -> "10.10.5.63";
ip_for_node("apollo-2-4") -> "10.10.5.64";
ip_for_node("apollo-2-5") -> "10.10.5.65";
ip_for_node("apollo-2-6") -> "10.10.5.66";
ip_for_node("apollo-2-7") -> "10.10.5.67";
ip_for_node("apollo-2-8") -> "10.10.5.68";
ip_for_node("apollo-2-9") -> "10.10.5.69";
ip_for_node("apollo-2-10") -> "10.10.5.70";
ip_for_node("apollo-2-11") -> "10.10.5.71";
ip_for_node("apollo-2-12") -> "10.10.5.72";
ip_for_node([$i ,$p, $- | Rest]) ->
    %% For aws, node names are ip-XXX-XXX-XXX-XXX
    %% We trim ip- prefix and change the dashes with points
    string:join(string:replace(Rest, "-", "."), "").

get_log_file(Path) ->
    {ok, Hostname} = inet:gethostname(),
    filename:join([Path, Hostname ++ ".imdea.log"]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% config
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
get_config_key(Key, Config) ->
    case lists:keyfind(Key, 1, Config) of
        false -> error;
        {Key, Value} -> {ok, Value}
    end.

get_config_key(Key, Config, Default) ->
    case lists:keyfind(Key, 1, Config) of
        false -> Default;
        {Key, Value} -> Value
    end.

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
        [$c] ->
            parse_flag(Flag, Args, fun(Arg) -> parse_command(Arg, Acc) end);
        [$v] ->
            parse_args(Args, Acc#{verbose => true});
        [$d] ->
            parse_args(Args, Acc#{dry_run => true});
        _ ->
            error
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
    Required = [config, command],
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

nonl(S) -> string:trim(S, trailing, "$\n").
