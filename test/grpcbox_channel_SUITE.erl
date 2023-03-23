-module(grpcbox_channel_SUITE).

-export([all/0,
        init_per_suite/1,
        end_per_suite/1,
        add_and_remove_endpoints/1,
        pick_worker_strategy/1]).

-include_lib("eunit/include/eunit.hrl").

all() ->
    [
        add_and_remove_endpoints,
        pick_worker_strategy
    ].
init_per_suite(_Config) ->
    application:set_env(grpcbox, servers, []),
    application:ensure_all_started(grpcbox),
    grpcbox_channel_sup:start_link(),
    grpcbox_channel_sup:start_child(default_channel, [{https, "127.0.0.1", 8080, #{}}], #{}),
    grpcbox_channel_sup:start_child(random_channel,
                                    [{https, "127.0.0.1", 8080, #{}}, {https, "127.0.0.2", 8080, #{}}, {https, "127.0.0.3", 8080, #{}}, {https, "127.0.0.4", 8080, #{}}],
                                    #{balancer => random}),
    grpcbox_channel_sup:start_child(hash_channel,
                                    [{https, "127.0.0.1", 8080, #{}}, {https, "127.0.0.2", 8080, #{}}, {https, "127.0.0.3", 8080, #{}}, {https, "127.0.0.4", 8080, #{}}],
                                    #{balancer => hash}),
    grpcbox_channel_sup:start_child(direct_channel,
                                    [{https, "127.0.0.1", 8080, #{}}, {https, "127.0.0.2", 8080, #{}}, {https, "127.0.0.3", 8080, #{}}, {https, "127.0.0.4", 8080, #{}}],
                                    #{ balancer => direct}),

    _Config.

end_per_suite(_Config) ->
    application:stop(grpcbox),
    ok.

add_and_remove_endpoints(_Config) ->
    grpcbox_channel:add_endpoints(default_channel, [{https, "127.0.0.2", 8080, #{}}, {https, "127.0.0.3", 8080, #{}}, {https, "127.0.0.4", 8080, #{}}]),
    ?assertEqual(4, length(gproc_pool:active_workers(default_channel))),
    grpcbox_channel:remove_endpoints(default_channel, [{https, "127.0.0.1", 8080, #{}}, {https, "127.0.0.2", 8080, #{}}, {https, "127.0.0.4", 8080, #{}}], normal),
    ?assertEqual(1, length(gproc_pool:active_workers(default_channel))).

pick_worker_strategy(_Config) ->
    ?assertEqual(ok, pick_worker(default_channel)),
    ?assertEqual(ok, pick_worker(random_channel)),
    ?assertEqual(ok, pick_worker(direct_channel, 1)),
    ?assertEqual(ok, pick_worker(hash_channel, 1)),
    ?assertEqual(error, pick_worker(default_channel, 1)),
    ?assertEqual(error, pick_worker(random_channel, 1)),
    ?assertEqual(error, pick_worker(direct_channel)),
    ?assertEqual(error, pick_worker(hash_channel)),
    ok.

pick_worker(Name, N) ->
    {R, _} = grpcbox_channel:pick(Name, unary, N),
    R.
pick_worker(Name) ->
    {R, _} = grpcbox_channel:pick(Name, unary, undefined),
    R.
