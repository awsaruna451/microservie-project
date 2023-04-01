package com.aruna.service;


import brave.Span;
import brave.Tracer;
import com.aruna.dto.InventoryResponse;
import com.aruna.dto.OrderLineItemsDto;
import com.aruna.dto.OrderRequest;
import com.aruna.event.OrderPlacedEvent;
import com.aruna.model.Order;
import com.aruna.model.OrderLineItems;
import com.aruna.repository.OrderRepository;

import lombok.RequiredArgsConstructor;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.reactive.function.client.WebClient;


import java.util.Arrays;
import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Transactional
public class OrderFirstService {

    private final OrderRepository orderRepository;
    private final WebClient.Builder webClientBuilder;
    private final Tracer tracer;
    private final KafkaTemplate<String,OrderPlacedEvent> kafkaTemplate;

    public String placeOrder(OrderRequest orderRequest) throws IllegalAccessException {
        Order order = new Order();
        order.setOrderNumber(UUID.randomUUID().toString());

        List<OrderLineItems> orderLineItems = orderRequest.getOrderLineItemsDtoList()
                .stream()
                .map(this::mapToDto)
                .toList();
        Span inventoryServiceLookup = tracer.nextSpan().name("InventoryServiceLookup");
        try (Tracer.SpanInScope isLookup = tracer.withSpanInScope(inventoryServiceLookup.start())) {
            order.setOrderLineItemsList(orderLineItems);
            inventoryServiceLookup.tag("call", "inventory-service");
            List<String> skuCodes = order.getOrderLineItemsList().stream()
                    .map(OrderLineItems::getSkuCode)
                    .toList();
            InventoryResponse[] inventoryResponsArray = webClientBuilder.build().get().uri("http://inventory-service/api/inventory"
                            , uriBuilder -> uriBuilder.queryParam("skuCode", skuCodes).build())
                    .retrieve().bodyToMono(InventoryResponse[].class)
                    .block();
            boolean allProductsInStock = Arrays.stream(inventoryResponsArray)
                    .allMatch(InventoryResponse::isInStock);


            if (allProductsInStock) {

                orderRepository.save(order);
                kafkaTemplate.send("notificationTopic", new OrderPlacedEvent(order.getOrderNumber()));
                return "Order placed Successfully";
            } else {
                throw new IllegalAccessException("Product not in stock, Please try again later");
            }
        }finally {
            inventoryServiceLookup.flush();
        }


    }

    private OrderLineItems mapToDto(OrderLineItemsDto orderLineItemsDto) {
        OrderLineItems orderLineItems = new OrderLineItems();
        orderLineItems.setPrice(orderLineItemsDto.getPrice());
        orderLineItems.setQuantity(orderLineItemsDto.getQuantity());
        orderLineItems.setSkuCode(orderLineItemsDto.getSkuCode());
        return orderLineItems;
    }
}
