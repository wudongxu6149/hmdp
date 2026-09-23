-- 先执行此脚本，再部署使用 close_refund_pending 字段的新版本。
-- 默认 0：已有的历史订单不自动重复回补；新的关单事务会显式置 1。
USE hmdp;

ALTER TABLE tb_voucher_order
    ADD COLUMN close_refund_pending tinyint(1) NOT NULL DEFAULT 0
        COMMENT '1=关单已提交但 Redis 回补待完成，0=无需回补或已完成',
    ADD INDEX idx_close_refund_pending (close_refund_pending, status, id);
