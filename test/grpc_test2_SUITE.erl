%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(grpc_test2_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include("grpc.hrl").
-include("test_helpers.hrl").

-define(SERVER_NAME, server).
-define(SERVER_ADDR, "http://127.0.0.1:10000").
-define(CHANN_NAME, channel).

-undef(LOG).
-define(LOG(Fmt, Args), io:format(standard_error, Fmt, Args)).

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    lists:usort([
        F
     || {F, 1} <- ?MODULE:module_info(exports),
        string:substr(atom_to_list(F), 1, 2) == "t_"
    ]).

init_per_suite(Cfg) ->
    _ = application:ensure_all_started(grpc),
    Services = #{protos => [grpc_test_pb], services => #{'Test' => test_svr}},
    [{services, Services} | Cfg].

end_per_suite(_Cfg) ->
    _ = application:stop(grpc).

init_per_testcase(t_deadline, Cfg) ->
    {ok, _} = grpc:start_server(?SERVER_NAME, 10000, ?config(services, Cfg),
                                [{ranch_opts, #{shutdown => brutal_kill}}]),
    {ok, _} = grpc_client_sup:create_channel_pool(?CHANN_NAME, ?SERVER_ADDR, #{}),
    Cfg;
init_per_testcase(_TestCase, Cfg) ->
    %% The case will handle the channel pool creation and server start by itself
    Cfg.

end_per_testcase(_TestCase, _Cfg) ->
    _ = grpc_client_sup:stop_channel_pool(?CHANN_NAME),
    _ = grpc:stop_server(?SERVER_NAME),
    ok.

%%--------------------------------------------------------------------
%% Tests
%%--------------------------------------------------------------------

t_deadline(_) ->
    ?assertMatch({error, {deadline_exceeded, _}},
                 test_client:test_deadline(#{ms => 3000},
                                           #{channel => ?CHANN_NAME,
                                             timeout => 2000}
                                          )),
    receive
        Msg ->
            ?assert({should_not_receive_a_garbage_msg, Msg})
    after 3000 ->
              ok
    end.

t_health_check(Cfg) ->
    Services = ?config(services, Cfg),
    {ok, _} = grpc:start_server(?SERVER_NAME, 10000, Services,
                                [{ranch_opts, #{shutdown => brutal_kill}}]),
    {ok, _} = grpc_client_sup:create_channel_pool(?CHANN_NAME, ?SERVER_ADDR, #{}),

    WorkersHealthCheck =
        fun(Worker) ->
            case grpc_client:health_check(Worker, #{channel => ?CHANN_NAME}) of
                ok -> true;
                _ -> false
            end
        end,

    WorkersPid = [WorkerPid || {_, WorkerPid} <- grpc_client_sup:workers(?CHANN_NAME)],

    ?assert(lists:all(WorkersHealthCheck, WorkersPid)),


    grpc:stop_server(?SERVER_NAME),
    ct:sleep(100),
    ?assertNot(lists:all(WorkersHealthCheck, WorkersPid)),

    _ = grpc_client_sup:stop_channel_pool(?CHANN_NAME),

    ok.

t_close_stream(_TCConfig) ->
    Services = #{protos => [grpc_test_pb], services => #{'Test' => test2_svr}},
    {ok, _} = grpc:start_server(?SERVER_NAME, 10000, Services,
                                [{ranch_opts, #{shutdown => brutal_kill}}]),
    {ok, _} = grpc_client_sup:create_channel_pool(?CHANN_NAME, ?SERVER_ADDR, #{}),
    TestPidBin = iolist_to_binary(pid_to_list(self())),

    %% call
    {ok, Stream1} =
        test_client:test_stream_out(#{<<"test_pid">> => TestPidBin},
                                    #{channel => ?CHANN_NAME,
                                      timeout => 2000}
                                   ),
    MRef1 =
        receive
            {grpc_req_enter, HandlerPid1, _GRPCReq1, _Meta1} ->
                monitor(process, HandlerPid1)
        after 3_000 ->
                ct:fail("didn't enter the handler")
        end,
    ok = grpc_client:close(Stream1, #{}),
    receive
        {grpc_exit_signal, Reason1} ->
            ct:pal("handler receive exit signal: ~p", [Reason1]),
            ok
    after 3_000 ->
        ct:fail("handler didn't receive exit")
    end,
    receive
        {'DOWN', MRef1, _, _, _} ->
            ok
    after 3_000 ->
        ct:fail("handler was not cancelled")
    end,

    %% cast
    {ok, Stream2} =
        test_client:test_stream_out(#{<<"test_pid">> => TestPidBin},
                                    #{channel => ?CHANN_NAME,
                                      timeout => 2000}
                                   ),
    MRef2 =
        receive
            {grpc_req_enter, HandlerPid2, _GRPCReq2, _Meta2} ->
                monitor(process, HandlerPid2)
        after 3_000 ->
                ct:fail("didn't enter the handler")
        end,
    ok = grpc_client:close_async(Stream2),
    receive
        {grpc_exit_signal, Reason2} ->
            ct:pal("handler receive exit signal: ~p", [Reason2]),
            ok
    after 3_000 ->
        ct:fail("handler didn't receive exit")
    end,
    receive
        {'DOWN', MRef2, _, _, _} ->
            ok
    after 3_000 ->
        ct:fail("handler was not cancelled")
    end,

    ok.

t_recv_async_once(_TCConfig) ->
    Services = #{protos => [grpc_test_pb], services => #{'Test' => test2_svr}},
    {ok, _} = grpc:start_server(?SERVER_NAME, 10000, Services,
                                [{ranch_opts, #{shutdown => brutal_kill}}]),
    {ok, _} = grpc_client_sup:create_channel_pool(?CHANN_NAME, ?SERVER_ADDR, #{}),
    TestPidBin = iolist_to_binary(pid_to_list(self())),

    {ok, Stream1} =
        test_client:test_stream_out(#{<<"test_pid">> => TestPidBin},
                                    #{channel => ?CHANN_NAME,
                                      timeout => 2000}
                                   ),
    {grpc_req_enter, HandlerPid1, GRPCReq1, _Meta1} =
        ?assertReceive({grpc_req_enter, _, _, _}),

    Handle1 = grpc_client:recv_async(Stream1, #{mode => once}),

    %% even though we set mode to once, we may receive a batch of replies (including
    %% trailers if the server happens to close the connection).
    grpc_stream:reply(GRPCReq1, [
        #{message => <<"1">>},
        #{message => <<"2">>},
        #{message => <<"3">>}
    ]),

    {grpc_reply, _, {ok, Reply1Raw}} = ?assertReceive({grpc_reply, Handle1, _}),
    ?assertMatch(
       [ #{message := <<"1">>}
       , #{message := <<"2">>}
       , #{message := <<"3">>}
       ],
       grpc_client:map_recv_async_reply(Stream1, Reply1Raw)
      ),

    %% further replies shouldn't be received unless asked for.
    grpc_stream:reply(GRPCReq1, [
        #{message => <<"4">>},
        #{message => <<"5">>},
        #{message => <<"6">>}
    ]),
    ?assertNotReceive({grpc_reply, _, _}),

    Handle2 = grpc_client:recv_async(Stream1, #{mode => once}),
    {grpc_reply, _, {ok, Reply2Raw}} = ?assertReceive({grpc_reply, Handle2, _}),
    ?assertMatch(
       [ #{message := <<"4">>}
       , #{message := <<"5">>}
       , #{message := <<"6">>}
       ],
       grpc_client:map_recv_async_reply(Stream1, Reply2Raw)
      ),

    grpc_stream:reply(GRPCReq1, [
        #{message => <<"7">>},
        #{message => <<"8">>},
        #{message => <<"9">>}
    ]),
    ?assertNotReceive({grpc_reply, _, _}),

    HandlerPid1 ! continue,

    Handle3 = grpc_client:recv_async(Stream1, #{mode => once}),
    {grpc_reply, _, {ok, Reply3Raw}} = ?assertReceive({grpc_reply, Handle3, _}),
    %% race: might receive trailers bundled with batch, or later.
    case grpc_client:map_recv_async_reply(Stream1, Reply3Raw) of
        [ #{message := <<"7">>}
        , #{message := <<"8">>}
        , #{message := <<"9">>}
        ] ->
            ct:pal("waiting for trailers"),
            Handle4 = grpc_client:recv_async(Stream1, #{mode => once}),
            {grpc_reply, _, {ok, Reply4Raw}} = ?assertReceive({grpc_reply, Handle4, _}),
            ?assertMatch(
               [{eos, [{<<"grpc-status">>, ?GRPC_STATUS_OK}]}],
               grpc_client:map_recv_async_reply(Stream1, Reply4Raw)
              ),
            ok;
        [ #{message := <<"7">>}
        , #{message := <<"8">>}
        , #{message := <<"9">>}
        , {eos, [{<<"grpc-status">>, ?GRPC_STATUS_OK}]}
        ] ->
            ct:pal("got trailers"),
            ok;
        Unexpected1 ->
            error({unexpected_reply, Unexpected1})
    end,
    ?assertNotReceive({grpc_reply, Handle2, _}),

    %% stream is already gone.
    Handle5 = grpc_client:recv_async(Stream1, #{mode => once}),
    ?assertReceive({grpc_reply, Handle5, {error, not_found}}),

    %% should be notified if client dies.
    {ok, Stream2} =
        test_client:test_stream_out(#{<<"test_pid">> => TestPidBin},
                                    #{channel => ?CHANN_NAME,
                                      timeout => 2000}
                                   ),
    {grpc_req_enter, _HandlerPid2, _GRPCReq2, _Meta2} =
        ?assertReceive({grpc_req_enter, _, _, _}),

    ReplyAlias2 = grpc_client:recv_async(Stream2, #{mode => once}),

    #{client_pid := ClientPid2} = Stream2,
    exit(ClientPid2, kill),

    ?assertReceive({'DOWN', ReplyAlias2, process, ClientPid2, _}),

    ok.

t_recv_async_active(_TCConfig) ->
    Services = #{protos => [grpc_test_pb], services => #{'Test' => test2_svr}},
    {ok, _} = grpc:start_server(?SERVER_NAME, 10000, Services,
                                [{ranch_opts, #{shutdown => brutal_kill}}]),
    {ok, _} = grpc_client_sup:create_channel_pool(?CHANN_NAME, ?SERVER_ADDR, #{}),
    TestPidBin = iolist_to_binary(pid_to_list(self())),

    {ok, Stream1} =
        test_client:test_stream_out(#{<<"test_pid">> => TestPidBin},
                                    #{channel => ?CHANN_NAME,
                                      timeout => 2000}
                                   ),
    {grpc_req_enter, HandlerPid1, GRPCReq1, _Meta1} =
        ?assertReceive({grpc_req_enter, _, _, _}),

    %% set up some replies before setting recv async owner.
    grpc_stream:reply(GRPCReq1, [
        #{message => <<"1">>},
        #{message => <<"2">>},
        #{message => <<"3">>}
    ]),

    Handle1 = grpc_client:recv_async(Stream1, #{mode => active}),

    {grpc_reply, _, {ok, Reply1Raw}} = ?assertReceive({grpc_reply, Handle1, _}),
    ?assertMatch(
       [ #{message := <<"1">>}
       , #{message := <<"2">>}
       , #{message := <<"3">>}
       ],
       grpc_client:map_recv_async_reply(Stream1, Reply1Raw)
      ),

    grpc_stream:reply(GRPCReq1, [
        #{message => <<"4">>},
        #{message => <<"5">>},
        #{message => <<"6">>}
    ]),

    {grpc_reply, _, {ok, Reply2Raw}} = ?assertReceive({grpc_reply, Handle1, _}),
    ?assertMatch(
       [ #{message := <<"4">>}
       , #{message := <<"5">>}
       , #{message := <<"6">>}
       ],
       grpc_client:map_recv_async_reply(Stream1, Reply2Raw)
      ),

    grpc_stream:reply(GRPCReq1, [
        #{message => <<"7">>},
        #{message => <<"8">>},
        #{message => <<"9">>}
    ]),
    HandlerPid1 ! continue,
    {grpc_reply, _, {ok, Reply3Raw}} = ?assertReceive({grpc_reply, Handle1, _}),
    %% race: might receive trailers bundled with batch, or later.
    case grpc_client:map_recv_async_reply(Stream1, Reply3Raw) of
        [ #{message := <<"7">>}
        , #{message := <<"8">>}
        , #{message := <<"9">>}
        ] ->
            ct:pal("waiting for trailers"),
            {grpc_reply, _, {ok, Reply4Raw}} = ?assertReceive({grpc_reply, Handle1, _}),
            ?assertMatch(
               [{eos, [{<<"grpc-status">>, ?GRPC_STATUS_OK}]}],
               grpc_client:map_recv_async_reply(Stream1, Reply4Raw)
              ),
            ok;
        [ #{message := <<"7">>}
        , #{message := <<"8">>}
        , #{message := <<"9">>}
        , {eos, [{<<"grpc-status">>, ?GRPC_STATUS_OK}]}
        ] ->
            ct:pal("got trailers"),
            ok;
        Unexpected1 ->
            error({unexpected_reply, Unexpected1})
    end,

    %% stream is already gone.
    Handle2 = grpc_client:recv_async(Stream1, #{mode => once}),
    ?assertReceive({grpc_reply, Handle2, {error, not_found}}),

    %% should be notified if client dies.
    {ok, Stream2} =
        test_client:test_stream_out(#{<<"test_pid">> => TestPidBin},
                                    #{channel => ?CHANN_NAME,
                                      timeout => 2000}
                                   ),
    {grpc_req_enter, _HandlerPid2, _GRPCReq2, _Meta2} =
        ?assertReceive({grpc_req_enter, _, _, _}),

    ReplyAlias2 = grpc_client:recv_async(Stream2, #{mode => once}),

    #{client_pid := ClientPid2} = Stream2,
    exit(ClientPid2, kill),

    ?assertReceive({'DOWN', ReplyAlias2, process, ClientPid2, _}),

    ok.
