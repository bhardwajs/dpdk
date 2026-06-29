/* SPDX-License-Identifier: MIT
 * Copyright(c) 2026 Sumit Bhardwaj <bhardwajs@outlook.com>
 */

#include <inttypes.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>

#include <rte_cycles.h>
#include <rte_eal.h>
#include <rte_launch.h>
#include <rte_lcore.h>
#include <rte_malloc.h>
#include <rte_random.h>
#include <rte_rcu_qsbr.h>
#include <rte_stdatomic.h>
#include <rte_thread.h>

#define RCU_QSBR_NUM_WORKERS 4
#define RCU_QSBR_WORK_PER_QS 1024
#define RCU_QSBR_SYNC_ROUNDS 8
#define RCU_QSBR_CONTROL_MAX_DELAY_MS 2000
#define RCU_QSBR_WORK_MAX_DELAY_US 1000
#define RCU_QSBR_OBSERVER_MAX_INTERVAL_MS 5000

struct worker_context {
	uint16_t thread_id;
	unsigned int lcore_id;
};

static struct rte_rcu_qsbr *rcu_qsbr;
static struct worker_context worker_contexts[RCU_QSBR_NUM_WORKERS];
static RTE_ATOMIC(uint16_t) quit_signal;
static RTE_ATOMIC(uint64_t) worker_qs_count[RCU_QSBR_NUM_WORKERS];

static void
do_work(uint16_t thread_id)
{
	uint32_t i;
	uint64_t work = thread_id;

	for (i = 0; i < RCU_QSBR_WORK_PER_QS; i++) {
		work += i + thread_id;
		rte_delay_us(rte_rand_max(RCU_QSBR_WORK_MAX_DELAY_US + 1));
	}

	if (work == 0)
		printf("worker %u unexpected work result\n", thread_id);
}

static int
worker_main(void *arg)
{
	struct worker_context *ctx = arg;

	printf("worker %u running on lcore %u\n", ctx->thread_id,
		ctx->lcore_id);

	while (rte_atomic_load_explicit(&quit_signal,
			rte_memory_order_relaxed) == 0) {
		do_work(ctx->thread_id);
		rte_rcu_qsbr_quiescent(rcu_qsbr, ctx->thread_id);
		rte_atomic_fetch_add_explicit(&worker_qs_count[ctx->thread_id],
			1, rte_memory_order_relaxed);
	}

	rte_rcu_qsbr_thread_offline(rcu_qsbr, ctx->thread_id);
	rte_rcu_qsbr_thread_unregister(rcu_qsbr, ctx->thread_id);
	printf("worker %u exiting\n", ctx->thread_id);

	return 0;
}

static uint32_t
control_thread_main(void *arg __rte_unused)
{
	uint64_t token;
	uint32_t round;

	for (round = 0; round < RCU_QSBR_SYNC_ROUNDS; round++) {
		if (rte_atomic_load_explicit(&quit_signal,
				rte_memory_order_relaxed) != 0)
			break;

		rte_delay_ms(rte_rand_max(RCU_QSBR_CONTROL_MAX_DELAY_MS + 1));
		token = rte_rcu_qsbr_start(rcu_qsbr);
		printf("control: synchronize round %u token=%" PRIu64 " start\n",
			round + 1, token);
		rte_rcu_qsbr_check(rcu_qsbr, token, true);
		printf("control: synchronize round %u token=%" PRIu64 " complete\n",
			round + 1, token);
	}

	rte_atomic_store_explicit(&quit_signal, 1, rte_memory_order_relaxed);
	return 0;
}

static uint64_t
monitor_qsbr(void)
{
	static bool tracking_started;
	static uint64_t last_acked_token;
	static uint64_t tracking_token;
	int ret;

	if (!tracking_started) {
		last_acked_token = rte_atomic_load_explicit(&rcu_qsbr->acked_token,
			rte_memory_order_relaxed);
		tracking_token = rte_rcu_qsbr_start(rcu_qsbr);
		tracking_started = true;
		return last_acked_token;
	}

	ret = rte_rcu_qsbr_check(rcu_qsbr, tracking_token, false);
	if (ret == 1) {
		last_acked_token = tracking_token;
		tracking_token = rte_rcu_qsbr_start(rcu_qsbr);
	}

	return last_acked_token;
}

static uint32_t
monitor_thread_main(void *arg __rte_unused)
{
	uint64_t acked_token;
	uint64_t previous_acked_token;

	previous_acked_token = monitor_qsbr();

	while (rte_atomic_load_explicit(&quit_signal,
			rte_memory_order_relaxed) == 0) {
		rte_delay_us_sleep(
			rte_rand_max(RCU_QSBR_OBSERVER_MAX_INTERVAL_MS + 1) * 1000);
		if (rte_atomic_load_explicit(&quit_signal,
				rte_memory_order_relaxed) != 0)
			break;

		acked_token = monitor_qsbr();
		if (acked_token != previous_acked_token)
			previous_acked_token = acked_token;
	}

	return 0;
}

static int
init_rcu(void)
{
	size_t size;
	uint16_t worker_id;

	size = rte_rcu_qsbr_get_memsize(RCU_QSBR_NUM_WORKERS);
	rcu_qsbr = rte_zmalloc("rcu_qsbr", size, RTE_CACHE_LINE_SIZE);
	if (rcu_qsbr == NULL) {
		printf("failed to allocate RCU QSBR variable\n");
		return -1;
	}

	if (rte_rcu_qsbr_init(rcu_qsbr, RCU_QSBR_NUM_WORKERS) != 0) {
		printf("failed to initialize RCU QSBR variable\n");
		rte_free(rcu_qsbr);
		rcu_qsbr = NULL;
		return -1;
	}

	for (worker_id = 0; worker_id < RCU_QSBR_NUM_WORKERS; worker_id++) {
		if (rte_rcu_qsbr_thread_register(rcu_qsbr, worker_id) != 0) {
			printf("failed to register worker %u\n", worker_id);
			return -1;
		}
		rte_rcu_qsbr_thread_online(rcu_qsbr, worker_id);
	}

	return 0;
}

static void
print_worker_stats(void)
{
	uint16_t worker_id;

	for (worker_id = 0; worker_id < RCU_QSBR_NUM_WORKERS; worker_id++)
		printf("worker %u reported %" PRIu64 " quiescent states\n",
			worker_id,
			rte_atomic_load_explicit(&worker_qs_count[worker_id],
				rte_memory_order_relaxed));
}

int
main(int argc, char **argv)
{
	rte_thread_t control_thread;
	rte_thread_t monitor_thread;
	uint32_t control_ret;
	uint32_t monitor_ret;
	unsigned int lcore_id;
	uint16_t worker_id = 0;
	int ret;

	ret = rte_eal_init(argc, argv);
	if (ret < 0)
		rte_exit(EXIT_FAILURE, "Cannot initialize EAL\n");

	if (rte_lcore_count() < RCU_QSBR_NUM_WORKERS)
		rte_exit(EXIT_FAILURE,
			"This example needs at least %u EAL lcores; try -l 0-3\n",
			RCU_QSBR_NUM_WORKERS);

	if (init_rcu() != 0)
		rte_exit(EXIT_FAILURE, "Cannot initialize RCU QSBR\n");

	RTE_LCORE_FOREACH(lcore_id) {
		if (worker_id == RCU_QSBR_NUM_WORKERS)
			break;
		worker_contexts[worker_id].thread_id = worker_id;
		worker_contexts[worker_id].lcore_id = lcore_id;
		worker_id++;
	}

	printf("Launching %u RCU QSBR workers. Run with -l 0-3 on a 4-core system.\n",
		RCU_QSBR_NUM_WORKERS);

	for (worker_id = 0; worker_id < RCU_QSBR_NUM_WORKERS; worker_id++) {
		if (worker_contexts[worker_id].lcore_id == rte_get_main_lcore())
			continue;

		ret = rte_eal_remote_launch(worker_main,
			&worker_contexts[worker_id],
			worker_contexts[worker_id].lcore_id);
		if (ret != 0)
			rte_exit(EXIT_FAILURE, "Cannot launch worker %u\n",
				worker_id);
	}

	ret = rte_thread_create_control(&control_thread, "rcu-control",
		control_thread_main, NULL);
	if (ret < 0)
		rte_exit(EXIT_FAILURE, "Cannot create control thread\n");

	ret = rte_thread_create_control(&monitor_thread, "rcu-monitor",
		monitor_thread_main, NULL);
	if (ret < 0)
		rte_exit(EXIT_FAILURE, "Cannot create monitor thread\n");

	for (worker_id = 0; worker_id < RCU_QSBR_NUM_WORKERS; worker_id++) {
		if (worker_contexts[worker_id].lcore_id == rte_get_main_lcore()) {
			worker_main(&worker_contexts[worker_id]);
			break;
		}
	}

	rte_eal_mp_wait_lcore();
	rte_thread_join(control_thread, &control_ret);
	rte_thread_join(monitor_thread, &monitor_ret);
	print_worker_stats();

	rte_free(rcu_qsbr);
	rte_eal_cleanup();

	return control_ret != 0 || monitor_ret != 0 ? EXIT_FAILURE : EXIT_SUCCESS;
}
