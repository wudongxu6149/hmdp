-- =============================================================
-- 黑马点评升级脚本（阶段0）：tb_voucher_order 一人一单硬约束
--
-- 方案A语义：一人同一时刻只持有一张"有效"订单，取消后可重抢。
--   active_flag = 0        表示有效订单
--   active_flag = 订单id   订单取消/超时关闭时置值，使其退出唯一约束
-- 唯一索引 (user_id, voucher_id, active_flag)：
--   有效单 (uid, vid, 0) 全表最多一行 → 一人一单的最终兜底；
--   已取消单 (uid, vid, 订单id) 互不冲突 → 关单后可重新下单。
--
-- 执行前建议备份：
--   CREATE TABLE tb_voucher_order_bak_20260918 AS SELECT * FROM tb_voucher_order;
-- =============================================================
USE hmdp;

-- 1) 清理历史重复数据（同一用户同一券保留最早一行），防止建唯一索引失败
--    当前表中无数据，此语句为防御性清理，重复导入 hmdp.sql 后也安全
DELETE o
FROM tb_voucher_order o
JOIN tb_voucher_order keep_row
  ON o.user_id = keep_row.user_id
 AND o.voucher_id = keep_row.voucher_id
 AND o.id <> keep_row.id
 AND (o.create_time > keep_row.create_time
      OR (o.create_time = keep_row.create_time AND o.id > keep_row.id));

-- 2) 新增 active_flag 列 + 一人一单唯一索引
ALTER TABLE tb_voucher_order
    ADD COLUMN active_flag bigint(20) UNSIGNED NOT NULL DEFAULT 0
        COMMENT '有效标记：0=有效订单；取消/超时关闭时置为订单id，退出唯一约束',
    ADD UNIQUE KEY uk_user_voucher_active (user_id, voucher_id, active_flag);

-- 回滚脚本（如需撤销）：
-- ALTER TABLE tb_voucher_order
--     DROP INDEX uk_user_voucher_active,
--     DROP COLUMN active_flag;
