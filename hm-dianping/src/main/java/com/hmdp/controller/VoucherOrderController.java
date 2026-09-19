package com.hmdp.controller;


import com.hmdp.dto.Result;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.service.IVoucherService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * <p>
 *  前端控制器
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@RestController
@RequestMapping("/voucher-order")
public class VoucherOrderController {

    private IVoucherOrderService iVoucherOrderService;
    public VoucherOrderController(IVoucherOrderService iVoucherOrderService){
        this.iVoucherOrderService=iVoucherOrderService;
    }

    @PostMapping("seckill/{id}")
    public Result seckillVoucher(@PathVariable("id") Long voucherId) {
        return iVoucherOrderService.seckillVoucher(voucherId);
    }

    /**
     * 【阶段2新增】秒杀下单结果轮询：客户端拿到 orderId 后据此获取终态
     * 返回 PENDING（处理中）、SUCCESS（已落库）、FAILED:原因、CLOSED:原因、UNKNOWN（不存在）
     */
    @GetMapping("/result/{orderId}")
    public Result seckillResult(@PathVariable("orderId") Long orderId) {
        return iVoucherOrderService.querySeckillResult(orderId);
    }

    /**
     * 【阶段3新增】模拟支付：CAS 条件更新 status 1→2。
     * 已关单/已支付时返回幂等冲突提示——用于验证「支付与关单并发竞争，一单一终态」
     */
    @PostMapping("/pay/{orderId}")
    public Result payOrder(@PathVariable("orderId") Long orderId) {
        return iVoucherOrderService.payOrder(orderId);
    }

}
