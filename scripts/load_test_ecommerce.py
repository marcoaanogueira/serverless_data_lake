#!/usr/bin/env python3
"""
Load Test Script - E-commerce End-to-End (VTEX-like Orders)

Simulates a realistic e-commerce data pipeline using the Serverless Data Lake APIs.
Generates orders, items, payments, and shipping data inspired by VTEX Orders API,
ingests it through the full pipeline, creates gold-layer transform jobs, and
queries analytics KPIs.

Usage:
    python scripts/load_test_ecommerce.py --base-url https://<api-gateway-id>.execute-api.<region>.amazonaws.com

    # Customize volume
    python scripts/load_test_ecommerce.py --base-url https://... --num-orders 5000 --batch-size 50

    # Skip endpoint creation (if already created)
    python scripts/load_test_ecommerce.py --base-url https://... --skip-setup

    # Only generate data (dry run, prints sample to stdout)
    python scripts/load_test_ecommerce.py --dry-run --num-orders 20

    # Run specific phases
    python scripts/load_test_ecommerce.py --base-url https://... --phase setup
    python scripts/load_test_ecommerce.py --base-url https://... --phase ingest
    python scripts/load_test_ecommerce.py --base-url https://... --phase transform
    python scripts/load_test_ecommerce.py --base-url https://... --phase query
"""

import argparse
import json
import random
import sys
import time
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Any

import requests

# =============================================================================
# Constants - Realistic Brazilian E-commerce Data
# =============================================================================

DOMAIN = "ecommerce"

# Order statuses with realistic distribution weights
ORDER_STATUSES = [
    ("invoiced", 45),          # 45% - completed orders
    ("payment_approved", 20),  # 20% - payment confirmed, awaiting fulfillment
    ("handling", 10),          # 10% - being prepared
    ("ready_for_handling", 5), # 5%  - ready to ship
    ("canceled", 8),           # 8%  - canceled
    ("payment_pending", 7),    # 7%  - awaiting payment
    ("cancellation_requested", 3),  # 3% - cancellation in progress
    ("window_to_cancel", 2),   # 2%  - within cancellation window
]

CANCELLATION_REASONS = [
    "customer_request",
    "payment_timeout",
    "out_of_stock",
    "fraud_detected",
    "address_issue",
    "duplicate_order",
    "price_error",
    "shipping_delay",
]

# Payment methods with realistic distribution
PAYMENT_METHODS = [
    ("credit_card", 55),
    ("pix", 25),
    ("boleto", 10),
    ("debit_card", 7),
    ("gift_card", 3),
]

CREDIT_CARD_BRANDS = ["visa", "mastercard", "elo", "amex", "hipercard"]

# Product catalog - categories, products, and price ranges (BRL)
PRODUCT_CATALOG = {
    "eletronicos": [
        ("Smartphone Samsung Galaxy S24", 2899.00, 4599.00),
        ("iPhone 15 Pro Max 256GB", 7499.00, 9999.00),
        ("Notebook Dell Inspiron 15", 3299.00, 5499.00),
        ("Smart TV LG 55 4K OLED", 3999.00, 6999.00),
        ("Fone Bluetooth JBL Tune 520BT", 199.00, 349.00),
        ("Tablet Samsung Galaxy Tab S9", 2499.00, 4299.00),
        ("Console PlayStation 5", 3799.00, 4499.00),
        ("Smartwatch Apple Watch Series 9", 3299.00, 5199.00),
        ("Caixa de Som JBL Charge 5", 699.00, 999.00),
        ("Camera GoPro Hero 12", 2199.00, 3299.00),
        ("Mouse Gamer Logitech G502", 249.00, 449.00),
        ("Teclado Mecanico Redragon Kumara", 179.00, 299.00),
        ("Monitor LG UltraWide 29", 1299.00, 1899.00),
        ("Echo Dot 5a Geracao", 249.00, 399.00),
        ("Kindle Paperwhite 11a Geracao", 549.00, 749.00),
    ],
    "moda": [
        ("Tenis Nike Air Max 90", 499.00, 899.00),
        ("Camiseta Polo Ralph Lauren", 299.00, 599.00),
        ("Calca Jeans Levis 501", 299.00, 499.00),
        ("Vestido Zara Midi Floral", 199.00, 399.00),
        ("Jaqueta Adidas Windbreaker", 349.00, 599.00),
        ("Bolsa Michael Kors Jet Set", 899.00, 1899.00),
        ("Oculos Ray-Ban Aviator", 499.00, 899.00),
        ("Relogio Casio G-Shock", 399.00, 799.00),
        ("Tenis Adidas Ultraboost", 599.00, 999.00),
        ("Mochila Nike Brasilia", 149.00, 249.00),
    ],
    "casa_e_jardim": [
        ("Cafeteira Nespresso Vertuo", 599.00, 999.00),
        ("Aspirador Robo iRobot Roomba", 1999.00, 3999.00),
        ("Panela Eletrica de Arroz Mondial", 129.00, 249.00),
        ("Fritadeira Air Fryer Philips", 399.00, 799.00),
        ("Liquidificador Vitamix E320", 699.00, 1299.00),
        ("Jogo de Cama King 400 fios", 299.00, 599.00),
        ("Travesseiro Nasa Viscoelastico", 99.00, 199.00),
        ("Luminaria LED de Mesa", 79.00, 179.00),
        ("Organizador de Closet", 149.00, 349.00),
        ("Churrasqueira Eletrica Arno", 199.00, 399.00),
    ],
    "beleza_e_saude": [
        ("Perfume Chanel N5 100ml", 799.00, 1599.00),
        ("Kit Skincare La Roche-Posay", 199.00, 399.00),
        ("Secador Dyson Supersonic", 2499.00, 3499.00),
        ("Escova Eletrica Oral-B Pro", 299.00, 599.00),
        ("Protetor Solar Neutrogena FPS70", 49.00, 89.00),
        ("Creme Hidratante Nivea 400ml", 29.00, 59.00),
        ("Maquiagem Paleta Urban Decay", 249.00, 449.00),
        ("Shampoo Kerastase Nutritive", 149.00, 299.00),
    ],
    "esporte_e_lazer": [
        ("Bicicleta Mountain Bike Caloi", 1499.00, 3499.00),
        ("Esteira Eletrica Movement", 2999.00, 5999.00),
        ("Kit Halteres Emborrachados 20kg", 199.00, 399.00),
        ("Barraca Camping NTK 4 pessoas", 299.00, 699.00),
        ("Prancha de Surf 6.0", 899.00, 1999.00),
        ("Patins Inline Rollerblade", 499.00, 999.00),
        ("Bola Futebol Adidas Official", 99.00, 249.00),
        ("Raquete Tenis Wilson Pro", 399.00, 899.00),
    ],
    "livros_e_papelaria": [
        ("Box Harry Potter 7 Volumes", 149.00, 249.00),
        ("Livro O Poder do Habito", 29.00, 59.00),
        ("Agenda Planner 2025", 49.00, 99.00),
        ("Kit Canetas Staedtler 36 cores", 79.00, 149.00),
        ("Livro Sapiens Yuval Harari", 39.00, 69.00),
        ("Caderno Inteligente A4", 59.00, 119.00),
    ],
    "mercado_e_alimentos": [
        ("Kit Whey Protein 2kg", 149.00, 299.00),
        ("Capsulas Nespresso 50 unid", 99.00, 179.00),
        ("Azeite Extra Virgem Italiano 500ml", 39.00, 89.00),
        ("Kit Cervejas Artesanais 12 unid", 79.00, 159.00),
        ("Chocolate Lindt 85% 100g", 19.00, 39.00),
        ("Cafe Especial Torrado 1kg", 49.00, 99.00),
    ],
    "bebes_e_criancas": [
        ("Carrinho Bebe Chicco Bravo", 1999.00, 3499.00),
        ("Lego Technic Bugatti", 999.00, 1999.00),
        ("Boneca Barbie Dreamhouse", 299.00, 599.00),
        ("Cadeirinha Auto Burigotto", 499.00, 999.00),
        ("Kit Fraldas Pampers G 200 unid", 149.00, 249.00),
        ("Brinquedo Educativo Montessori", 99.00, 199.00),
    ],
}

# Shipping carriers
CARRIERS = [
    ("correios_sedex", "Correios SEDEX", 15.90, 45.90),
    ("correios_pac", "Correios PAC", 9.90, 29.90),
    ("jadlog", "Jadlog", 12.90, 39.90),
    ("loggi", "Loggi", 14.90, 49.90),
    ("total_express", "Total Express", 11.90, 35.90),
    ("azul_cargo", "Azul Cargo Express", 19.90, 59.90),
]

# Brazilian states for addresses
BRAZILIAN_STATES = [
    ("SP", "Sao Paulo", 35),
    ("RJ", "Rio de Janeiro", 15),
    ("MG", "Minas Gerais", 12),
    ("RS", "Rio Grande do Sul", 7),
    ("PR", "Parana", 6),
    ("BA", "Bahia", 5),
    ("SC", "Santa Catarina", 4),
    ("PE", "Pernambuco", 3),
    ("CE", "Ceara", 3),
    ("DF", "Distrito Federal", 3),
    ("GO", "Goias", 2),
    ("PA", "Para", 2),
    ("ES", "Espirito Santo", 1),
    ("MA", "Maranhao", 1),
    ("AM", "Amazonas", 1),
]

FIRST_NAMES = [
    "Lucas", "Pedro", "Gabriel", "Rafael", "Matheus", "Gustavo", "Felipe",
    "Bruno", "Leonardo", "Thiago", "Maria", "Ana", "Juliana", "Fernanda",
    "Camila", "Larissa", "Amanda", "Patricia", "Beatriz", "Carolina",
    "Joao", "Carlos", "Ricardo", "Eduardo", "Marcelo", "Andre", "Diego",
    "Daniela", "Leticia", "Isabela", "Renata", "Tatiana", "Priscila",
    "Vinicius", "Rodrigo", "Fabio", "Alexandre", "Mariana", "Natalia",
    "Aline",
]

LAST_NAMES = [
    "Silva", "Santos", "Oliveira", "Souza", "Pereira", "Costa", "Rodrigues",
    "Almeida", "Nascimento", "Lima", "Araujo", "Fernandes", "Carvalho",
    "Gomes", "Martins", "Rocha", "Ribeiro", "Alves", "Monteiro", "Mendes",
    "Barros", "Freitas", "Barbosa", "Moreira", "Campos", "Cardoso",
    "Teixeira", "Vieira", "Pinto", "Dias",
]

STREET_NAMES = [
    "Rua das Flores", "Avenida Brasil", "Rua Sao Paulo", "Avenida Paulista",
    "Rua XV de Novembro", "Rua Augusta", "Avenida Atlantica", "Rua Oscar Freire",
    "Rua da Consolacao", "Avenida Rio Branco", "Rua Sete de Setembro",
    "Rua Voluntarios da Patria", "Avenida Presidente Vargas", "Rua Haddock Lobo",
    "Rua Padre Chagas", "Avenida Beira Mar", "Rua da Bahia", "Rua Parana",
]

SELLER_NAMES = [
    "TechStore Brasil", "MegaShop Online", "SuperOfertas", "LojaMax",
    "Eletro.com", "ModaExpress", "CasaDecor", "SportsWorld",
    "BelezaPura", "LivrariaDigital", "BabyKids Store", "NutriVida",
    "GadgetZone", "FashionHub", "HomeStyle", "PlayGames BR",
]

# Marketplace seller IDs
SELLERS = [(f"seller_{i:03d}", name) for i, name in enumerate(SELLER_NAMES, 1)]

# Coupon codes
COUPON_CODES = [
    "PRIMEIRA10", "VERAO2025", "FRETEGRATIS", "DESCONTO15",
    "BLACKFRIDAY", "NATAL20", "VOLTE10", "APP20",
    "PROMO30", "INDICOU15", "ANIVERSARIO", "BEMVINDO",
]


# =============================================================================
# Weighted random selection helper
# =============================================================================

def weighted_choice(items_with_weights: list[tuple]) -> Any:
    """Select an item based on distribution weights."""
    items = [x[0] for x in items_with_weights]
    weights = [x[1] for x in items_with_weights]
    return random.choices(items, weights=weights, k=1)[0]


def generate_cep(state_code: str) -> str:
    """Generate a realistic CEP (Brazilian postal code) for a given state."""
    cep_ranges = {
        "SP": (1000000, 19999999), "RJ": (20000000, 28999999),
        "MG": (30000000, 39999999), "RS": (90000000, 99999999),
        "PR": (80000000, 87999999), "BA": (40000000, 48999999),
        "SC": (88000000, 89999999), "PE": (50000000, 56999999),
        "CE": (60000000, 63999999), "DF": (70000000, 72799999),
        "GO": (72800000, 76799999), "PA": (66000000, 68899999),
        "ES": (29000000, 29999999), "MA": (65000000, 65999999),
        "AM": (69000000, 69299999),
    }
    low, high = cep_ranges.get(state_code, (1000000, 99999999))
    cep = random.randint(low, high)
    return f"{cep:08d}"


# =============================================================================
# Data Generators
# =============================================================================

class EcommerceDataGenerator:
    """Generates realistic e-commerce data for load testing."""

    def __init__(self, num_orders: int = 2000, start_date: str = "2024-07-01",
                 end_date: str = "2025-01-31"):
        self.num_orders = num_orders
        self.start_date = datetime.fromisoformat(start_date)
        self.end_date = datetime.fromisoformat(end_date)
        self.date_range_days = (self.end_date - self.start_date).days

        # Pre-generate customer pool (customers can have multiple orders)
        self.num_customers = max(int(num_orders * 0.6), 100)
        self.customers = self._generate_customer_pool()

        # Track generated data for cross-referencing
        self.orders: list[dict] = []
        self.order_items: list[dict] = []
        self.order_payments: list[dict] = []
        self.order_shipping: list[dict] = []

    def _generate_customer_pool(self) -> list[dict]:
        """Generate a pool of unique customers."""
        customers = []
        for i in range(self.num_customers):
            state_code, state_name, _ = random.choices(
                BRAZILIAN_STATES, weights=[s[2] for s in BRAZILIAN_STATES], k=1
            )[0]
            first = random.choice(FIRST_NAMES)
            last = random.choice(LAST_NAMES)
            customers.append({
                "customer_id": f"cust_{uuid.uuid4().hex[:12]}",
                "first_name": first,
                "last_name": last,
                "email": f"{first.lower()}.{last.lower()}{random.randint(1, 999)}@email.com",
                "document": f"{random.randint(100, 999)}.{random.randint(100, 999)}.{random.randint(100, 999)}-{random.randint(10, 99)}",
                "phone": f"+55{random.randint(11, 99)}{random.randint(900000000, 999999999)}",
                "state_code": state_code,
                "state_name": state_name,
                "city": state_name,  # Simplified
                "street": random.choice(STREET_NAMES),
                "number": str(random.randint(1, 5000)),
                "cep": generate_cep(state_code),
                "is_corporate": random.random() < 0.08,
            })
        return customers

    def _random_datetime(self) -> datetime:
        """Generate a random datetime within the date range, with daily seasonality."""
        day_offset = random.randint(0, self.date_range_days)
        base = self.start_date + timedelta(days=day_offset)
        # More orders during evening/night (18-23h) and lunch (11-14h)
        hour_weights = [
            1, 1, 0.5, 0.3, 0.2, 0.3,   # 00-05
            0.5, 1, 2, 3, 4, 5,           # 06-11
            5, 5, 4, 3, 3, 4,             # 12-17
            5, 6, 7, 7, 5, 3,             # 18-23
        ]
        hour = random.choices(range(24), weights=hour_weights, k=1)[0]
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        return base.replace(hour=hour, minute=minute, second=second)

    def _pick_items_for_order(self) -> list[tuple[str, str, float]]:
        """Pick random items for an order. Returns list of (category, product_name, price)."""
        num_items = random.choices(
            [1, 2, 3, 4, 5, 6],
            weights=[40, 30, 15, 8, 5, 2],
            k=1,
        )[0]
        items = []
        for _ in range(num_items):
            category = random.choice(list(PRODUCT_CATALOG.keys()))
            product = random.choice(PRODUCT_CATALOG[category])
            name, price_low, price_high = product
            price = round(random.uniform(price_low, price_high), 2)
            items.append((category, name, price))
        return items

    def generate_all(self) -> None:
        """Generate all e-commerce data."""
        sequence_counter = 100000

        for i in range(self.num_orders):
            sequence_counter += 1
            order_id = f"v{random.randint(50000000, 99999999)}ffets-01"
            sequence_number = str(sequence_counter)

            customer = random.choice(self.customers)
            created_at = self._random_datetime()
            status = weighted_choice(ORDER_STATUSES)

            # Pick items
            picked_items = self._pick_items_for_order()
            subtotal = sum(price for _, _, price in picked_items)
            total_quantity = sum(
                random.randint(1, 3) for _ in picked_items
            )

            # Discount logic: 25% of orders have a coupon
            discount_value = 0.0
            coupon_code = None
            if random.random() < 0.25:
                coupon_code = random.choice(COUPON_CODES)
                discount_pct = random.choice([5, 10, 15, 20, 30])
                discount_value = round(subtotal * discount_pct / 100, 2)

            # Shipping
            carrier_id, carrier_name, ship_low, ship_high = random.choice(CARRIERS)
            shipping_value = round(random.uniform(ship_low, ship_high), 2)
            # Free shipping for orders > R$299
            if subtotal > 299 and random.random() < 0.4:
                shipping_value = 0.0

            total_value = round(subtotal - discount_value + shipping_value, 2)
            total_value = max(total_value, 0.01)

            # Seller
            seller_id, seller_name = random.choice(SELLERS)

            # Cancellation data
            cancellation_reason = None
            cancelled_at = None
            if status in ("canceled", "cancellation_requested"):
                cancellation_reason = random.choice(CANCELLATION_REASONS)
                cancelled_at = (created_at + timedelta(
                    hours=random.randint(1, 72)
                )).isoformat()

            # Invoiced/shipped timestamps
            invoiced_at = None
            shipped_at = None
            delivered_at = None
            if status == "invoiced":
                invoiced_at = (created_at + timedelta(
                    hours=random.randint(1, 48)
                )).isoformat()
                shipped_at = (created_at + timedelta(
                    hours=random.randint(24, 96)
                )).isoformat()
                if random.random() < 0.85:
                    delivered_at = (created_at + timedelta(
                        days=random.randint(2, 15)
                    )).isoformat()

            # === ORDER record ===
            order = {
                "order_id": order_id,
                "sequence_number": sequence_number,
                "customer_id": customer["customer_id"],
                "customer_name": f"{customer['first_name']} {customer['last_name']}",
                "customer_email": customer["email"],
                "customer_document": customer["document"],
                "customer_phone": customer["phone"],
                "customer_is_corporate": customer["is_corporate"],
                "status": status,
                "creation_date": created_at.isoformat(),
                "last_change": (created_at + timedelta(
                    hours=random.randint(0, 168)
                )).isoformat(),
                "seller_id": seller_id,
                "seller_name": seller_name,
                "total_items": len(picked_items),
                "total_quantity": total_quantity,
                "subtotal_value": subtotal,
                "discount_value": discount_value,
                "shipping_value": shipping_value,
                "total_value": total_value,
                "coupon_code": coupon_code or "",
                "shipping_state": customer["state_code"],
                "shipping_city": customer["city"],
                "shipping_street": customer["street"],
                "shipping_number": customer["number"],
                "shipping_cep": customer["cep"],
                "carrier_id": carrier_id,
                "carrier_name": carrier_name,
                "cancellation_reason": cancellation_reason or "",
                "cancelled_at": cancelled_at or "",
                "invoiced_at": invoiced_at or "",
                "shipped_at": shipped_at or "",
                "delivered_at": delivered_at or "",
                "origin": random.choice(["website", "app_android", "app_ios", "marketplace"]),
                "marketplace_name": random.choice(["", "", "", "mercado_livre", "amazon_br", "magazine_luiza"]),
                "is_completed": status == "invoiced",
            }
            self.orders.append(order)

            # === ORDER ITEMS records ===
            for item_idx, (category, product_name, unit_price) in enumerate(picked_items):
                quantity = random.randint(1, 3)
                sku_id = f"sku_{uuid.uuid4().hex[:8]}"
                item = {
                    "item_id": f"{order_id}_item_{item_idx}",
                    "order_id": order_id,
                    "sku_id": sku_id,
                    "product_name": product_name,
                    "category": category,
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "total_price": round(unit_price * quantity, 2),
                    "discount_per_unit": round(discount_value / len(picked_items) / quantity, 2) if discount_value > 0 else 0.0,
                    "seller_id": seller_id,
                    "creation_date": created_at.isoformat(),
                    "is_gift": random.random() < 0.03,
                    "refund_value": round(unit_price * quantity, 2) if status == "canceled" else 0.0,
                }
                self.order_items.append(item)

            # === ORDER PAYMENTS records ===
            payment_method = weighted_choice(PAYMENT_METHODS)
            installments = 1
            card_brand = ""
            card_last_digits = ""
            if payment_method == "credit_card":
                installments = random.choices(
                    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12],
                    weights=[30, 10, 15, 8, 8, 10, 3, 3, 3, 5, 5],
                    k=1,
                )[0]
                card_brand = random.choice(CREDIT_CARD_BRANDS)
                card_last_digits = f"{random.randint(1000, 9999)}"

            payment = {
                "payment_id": f"pay_{uuid.uuid4().hex[:12]}",
                "order_id": order_id,
                "payment_method": payment_method,
                "card_brand": card_brand,
                "card_last_digits": card_last_digits,
                "installments": installments,
                "installment_value": round(total_value / installments, 2) if installments > 1 else total_value,
                "total_value": total_value,
                "payment_status": "approved" if status not in ("payment_pending", "canceled") else ("pending" if status == "payment_pending" else "refunded"),
                "transaction_id": f"txn_{uuid.uuid4().hex[:16]}",
                "authorization_code": f"{random.randint(100000, 999999)}",
                "nsu": f"{random.randint(1000000000, 9999999999)}",
                "tid": f"{random.randint(10000000000, 99999999999)}",
                "creation_date": created_at.isoformat(),
                "authorization_date": (created_at + timedelta(seconds=random.randint(5, 120))).isoformat() if status != "payment_pending" else "",
            }
            self.order_payments.append(payment)

            # === ORDER SHIPPING records ===
            estimated_days = random.randint(3, 15)
            shipping = {
                "shipping_id": f"ship_{uuid.uuid4().hex[:12]}",
                "order_id": order_id,
                "carrier_id": carrier_id,
                "carrier_name": carrier_name,
                "shipping_method": random.choice(["express", "standard", "economic"]),
                "shipping_value": shipping_value,
                "weight_kg": round(random.uniform(0.1, 30.0), 2),
                "width_cm": round(random.uniform(5, 100), 1),
                "height_cm": round(random.uniform(5, 80), 1),
                "length_cm": round(random.uniform(10, 120), 1),
                "tracking_number": f"BR{uuid.uuid4().hex[:11].upper()}" if status in ("invoiced", "handling") else "",
                "tracking_url": "",
                "estimated_delivery_days": estimated_days,
                "estimated_delivery_date": (created_at + timedelta(days=estimated_days)).strftime("%Y-%m-%d"),
                "shipped_at": shipped_at or "",
                "delivered_at": delivered_at or "",
                "receiver_name": f"{customer['first_name']} {customer['last_name']}",
                "state_code": customer["state_code"],
                "city": customer["city"],
                "cep": customer["cep"],
                "creation_date": created_at.isoformat(),
            }
            self.order_shipping.append(shipping)

    def get_summary(self) -> dict:
        """Get summary statistics of generated data."""
        statuses = defaultdict(int)
        for o in self.orders:
            statuses[o["status"]] += 1

        total_revenue = sum(o["total_value"] for o in self.orders if o["status"] == "invoiced")
        total_items_sold = sum(
            item["quantity"] for item in self.order_items
            if any(o["order_id"] == item["order_id"] and o["status"] == "invoiced"
                   for o in self.orders)
        )

        return {
            "total_orders": len(self.orders),
            "total_items": len(self.order_items),
            "total_payments": len(self.order_payments),
            "total_shipping": len(self.order_shipping),
            "unique_customers": len(set(o["customer_id"] for o in self.orders)),
            "date_range": f"{self.start_date.date()} to {self.end_date.date()}",
            "statuses": dict(statuses),
            "total_revenue_brl": round(total_revenue, 2),
            "total_items_sold": total_items_sold,
            "avg_order_value": round(total_revenue / max(statuses.get("invoiced", 1), 1), 2),
        }


# =============================================================================
# Endpoint Schema Definitions
# =============================================================================

ENDPOINT_SCHEMAS = {
    "orders": {
        "name": "orders",
        "domain": DOMAIN,
        "mode": "manual",
        "description": "Main orders table - VTEX-like order data with customer, shipping, and status info",
        "columns": [
            {"name": "order_id", "type": "string", "required": True, "primary_key": True, "description": "Unique VTEX-like order ID"},
            {"name": "sequence_number", "type": "string", "required": True, "primary_key": False, "description": "Sequential order number"},
            {"name": "customer_id", "type": "string", "required": True, "primary_key": False, "description": "Customer unique ID"},
            {"name": "customer_name", "type": "string", "required": True, "primary_key": False, "description": "Customer full name"},
            {"name": "customer_email", "type": "string", "required": True, "primary_key": False, "description": "Customer email"},
            {"name": "customer_document", "type": "string", "required": False, "primary_key": False, "description": "CPF/CNPJ"},
            {"name": "customer_phone", "type": "string", "required": False, "primary_key": False, "description": "Phone number"},
            {"name": "customer_is_corporate", "type": "boolean", "required": False, "primary_key": False, "description": "B2B flag"},
            {"name": "status", "type": "string", "required": True, "primary_key": False, "description": "Order status"},
            {"name": "creation_date", "type": "timestamp", "required": True, "primary_key": False, "description": "Order creation timestamp"},
            {"name": "last_change", "type": "timestamp", "required": False, "primary_key": False, "description": "Last modification timestamp"},
            {"name": "seller_id", "type": "string", "required": True, "primary_key": False, "description": "Seller ID"},
            {"name": "seller_name", "type": "string", "required": False, "primary_key": False, "description": "Seller display name"},
            {"name": "total_items", "type": "integer", "required": True, "primary_key": False, "description": "Number of distinct items"},
            {"name": "total_quantity", "type": "integer", "required": True, "primary_key": False, "description": "Total quantity of all items"},
            {"name": "subtotal_value", "type": "float", "required": True, "primary_key": False, "description": "Subtotal before discounts/shipping"},
            {"name": "discount_value", "type": "float", "required": False, "primary_key": False, "description": "Total discount amount"},
            {"name": "shipping_value", "type": "float", "required": False, "primary_key": False, "description": "Shipping cost"},
            {"name": "total_value", "type": "float", "required": True, "primary_key": False, "description": "Final order value"},
            {"name": "coupon_code", "type": "string", "required": False, "primary_key": False, "description": "Applied coupon code"},
            {"name": "shipping_state", "type": "string", "required": False, "primary_key": False, "description": "Destination state code"},
            {"name": "shipping_city", "type": "string", "required": False, "primary_key": False, "description": "Destination city"},
            {"name": "shipping_street", "type": "string", "required": False, "primary_key": False, "description": "Destination street"},
            {"name": "shipping_number", "type": "string", "required": False, "primary_key": False, "description": "Address number"},
            {"name": "shipping_cep", "type": "string", "required": False, "primary_key": False, "description": "CEP (postal code)"},
            {"name": "carrier_id", "type": "string", "required": False, "primary_key": False, "description": "Carrier identifier"},
            {"name": "carrier_name", "type": "string", "required": False, "primary_key": False, "description": "Carrier display name"},
            {"name": "cancellation_reason", "type": "string", "required": False, "primary_key": False, "description": "Reason for cancellation"},
            {"name": "cancelled_at", "type": "string", "required": False, "primary_key": False, "description": "Cancellation timestamp"},
            {"name": "invoiced_at", "type": "string", "required": False, "primary_key": False, "description": "Invoice timestamp"},
            {"name": "shipped_at", "type": "string", "required": False, "primary_key": False, "description": "Shipment timestamp"},
            {"name": "delivered_at", "type": "string", "required": False, "primary_key": False, "description": "Delivery timestamp"},
            {"name": "origin", "type": "string", "required": False, "primary_key": False, "description": "Order origin channel"},
            {"name": "marketplace_name", "type": "string", "required": False, "primary_key": False, "description": "External marketplace"},
            {"name": "is_completed", "type": "boolean", "required": False, "primary_key": False, "description": "Whether order is fully completed"},
        ],
    },
    "order_items": {
        "name": "order_items",
        "domain": DOMAIN,
        "mode": "manual",
        "description": "Order line items with product details, quantities, and prices",
        "columns": [
            {"name": "item_id", "type": "string", "required": True, "primary_key": True, "description": "Unique item line ID"},
            {"name": "order_id", "type": "string", "required": True, "primary_key": False, "description": "Parent order ID"},
            {"name": "sku_id", "type": "string", "required": True, "primary_key": False, "description": "SKU identifier"},
            {"name": "product_name", "type": "string", "required": True, "primary_key": False, "description": "Product display name"},
            {"name": "category", "type": "string", "required": True, "primary_key": False, "description": "Product category"},
            {"name": "quantity", "type": "integer", "required": True, "primary_key": False, "description": "Quantity ordered"},
            {"name": "unit_price", "type": "float", "required": True, "primary_key": False, "description": "Price per unit"},
            {"name": "total_price", "type": "float", "required": True, "primary_key": False, "description": "Total line price"},
            {"name": "discount_per_unit", "type": "float", "required": False, "primary_key": False, "description": "Discount per unit"},
            {"name": "seller_id", "type": "string", "required": False, "primary_key": False, "description": "Seller ID"},
            {"name": "creation_date", "type": "timestamp", "required": True, "primary_key": False, "description": "Creation timestamp"},
            {"name": "is_gift", "type": "boolean", "required": False, "primary_key": False, "description": "Whether this is a gift item"},
            {"name": "refund_value", "type": "float", "required": False, "primary_key": False, "description": "Refund value if canceled"},
        ],
    },
    "order_payments": {
        "name": "order_payments",
        "domain": DOMAIN,
        "mode": "manual",
        "description": "Payment transactions per order - methods, installments, authorization data",
        "columns": [
            {"name": "payment_id", "type": "string", "required": True, "primary_key": True, "description": "Unique payment ID"},
            {"name": "order_id", "type": "string", "required": True, "primary_key": False, "description": "Parent order ID"},
            {"name": "payment_method", "type": "string", "required": True, "primary_key": False, "description": "Payment method type"},
            {"name": "card_brand", "type": "string", "required": False, "primary_key": False, "description": "Card brand if credit/debit"},
            {"name": "card_last_digits", "type": "string", "required": False, "primary_key": False, "description": "Last 4 card digits"},
            {"name": "installments", "type": "integer", "required": True, "primary_key": False, "description": "Number of installments"},
            {"name": "installment_value", "type": "float", "required": True, "primary_key": False, "description": "Value per installment"},
            {"name": "total_value", "type": "float", "required": True, "primary_key": False, "description": "Total payment value"},
            {"name": "payment_status", "type": "string", "required": True, "primary_key": False, "description": "Payment status"},
            {"name": "transaction_id", "type": "string", "required": True, "primary_key": False, "description": "Gateway transaction ID"},
            {"name": "authorization_code", "type": "string", "required": False, "primary_key": False, "description": "Authorization code"},
            {"name": "nsu", "type": "string", "required": False, "primary_key": False, "description": "NSU number"},
            {"name": "tid", "type": "string", "required": False, "primary_key": False, "description": "TID from acquirer"},
            {"name": "creation_date", "type": "timestamp", "required": True, "primary_key": False, "description": "Payment creation timestamp"},
            {"name": "authorization_date", "type": "string", "required": False, "primary_key": False, "description": "Authorization timestamp"},
        ],
    },
    "order_shipping": {
        "name": "order_shipping",
        "domain": DOMAIN,
        "mode": "manual",
        "description": "Shipping and logistics data per order - carrier, tracking, dimensions",
        "columns": [
            {"name": "shipping_id", "type": "string", "required": True, "primary_key": True, "description": "Unique shipping record ID"},
            {"name": "order_id", "type": "string", "required": True, "primary_key": False, "description": "Parent order ID"},
            {"name": "carrier_id", "type": "string", "required": True, "primary_key": False, "description": "Carrier identifier"},
            {"name": "carrier_name", "type": "string", "required": True, "primary_key": False, "description": "Carrier display name"},
            {"name": "shipping_method", "type": "string", "required": True, "primary_key": False, "description": "Shipping speed tier"},
            {"name": "shipping_value", "type": "float", "required": True, "primary_key": False, "description": "Shipping cost"},
            {"name": "weight_kg", "type": "float", "required": False, "primary_key": False, "description": "Package weight in kg"},
            {"name": "width_cm", "type": "float", "required": False, "primary_key": False, "description": "Package width in cm"},
            {"name": "height_cm", "type": "float", "required": False, "primary_key": False, "description": "Package height in cm"},
            {"name": "length_cm", "type": "float", "required": False, "primary_key": False, "description": "Package length in cm"},
            {"name": "tracking_number", "type": "string", "required": False, "primary_key": False, "description": "Carrier tracking number"},
            {"name": "tracking_url", "type": "string", "required": False, "primary_key": False, "description": "Tracking page URL"},
            {"name": "estimated_delivery_days", "type": "integer", "required": False, "primary_key": False, "description": "Estimated days to deliver"},
            {"name": "estimated_delivery_date", "type": "date", "required": False, "primary_key": False, "description": "Estimated delivery date"},
            {"name": "shipped_at", "type": "string", "required": False, "primary_key": False, "description": "Shipment timestamp"},
            {"name": "delivered_at", "type": "string", "required": False, "primary_key": False, "description": "Delivery timestamp"},
            {"name": "receiver_name", "type": "string", "required": False, "primary_key": False, "description": "Name of receiver"},
            {"name": "state_code", "type": "string", "required": False, "primary_key": False, "description": "Destination state code"},
            {"name": "city", "type": "string", "required": False, "primary_key": False, "description": "Destination city"},
            {"name": "cep", "type": "string", "required": False, "primary_key": False, "description": "Destination CEP"},
            {"name": "creation_date", "type": "timestamp", "required": True, "primary_key": False, "description": "Creation timestamp"},
        ],
    },
}


# =============================================================================
# Gold Layer Transform Jobs - Analytics KPIs
# =============================================================================

TRANSFORM_JOBS = [
    {
        "domain": DOMAIN,
        "job_name": "daily_revenue",
        "query": """
            SELECT
                CAST(creation_date AS DATE) AS order_date,
                COUNT(DISTINCT order_id) AS total_orders,
                COUNT(DISTINCT customer_id) AS unique_customers,
                SUM(CASE WHEN status = 'invoiced' THEN total_value ELSE 0 END) AS gross_revenue,
                SUM(CASE WHEN status = 'invoiced' THEN discount_value ELSE 0 END) AS total_discounts,
                SUM(CASE WHEN status = 'invoiced' THEN shipping_value ELSE 0 END) AS total_shipping_revenue,
                SUM(CASE WHEN status = 'invoiced' THEN total_value ELSE 0 END)
                    - SUM(CASE WHEN status = 'invoiced' THEN discount_value ELSE 0 END) AS net_revenue,
                AVG(CASE WHEN status = 'invoiced' THEN total_value END) AS avg_order_value,
                COUNT(CASE WHEN status = 'canceled' THEN 1 END) AS canceled_orders,
                COUNT(CASE WHEN status = 'payment_pending' THEN 1 END) AS pending_payment_orders
            FROM ecommerce.silver.orders
            GROUP BY CAST(creation_date AS DATE)
            ORDER BY order_date
        """,
        "write_mode": "overwrite",
        "unique_key": "order_date",
        "schedule_type": "cron",
        "cron_schedule": "day",
    },
    {
        "domain": DOMAIN,
        "job_name": "product_performance",
        "query": """
            SELECT
                i.category,
                i.product_name,
                COUNT(DISTINCT i.order_id) AS order_count,
                SUM(i.quantity) AS units_sold,
                SUM(i.total_price) AS total_revenue,
                AVG(i.unit_price) AS avg_unit_price,
                SUM(i.refund_value) AS total_refunds,
                SUM(i.quantity * i.discount_per_unit) AS total_discounts_given,
                COUNT(CASE WHEN i.is_gift THEN 1 END) AS gift_count
            FROM ecommerce.silver.order_items i
            GROUP BY i.category, i.product_name
            ORDER BY total_revenue DESC
        """,
        "write_mode": "overwrite",
        "unique_key": "product_name",
        "schedule_type": "cron",
        "cron_schedule": "day",
    },
    {
        "domain": DOMAIN,
        "job_name": "category_summary",
        "query": """
            SELECT
                i.category,
                COUNT(DISTINCT i.order_id) AS total_orders,
                SUM(i.quantity) AS total_units_sold,
                SUM(i.total_price) AS total_revenue,
                AVG(i.unit_price) AS avg_price,
                COUNT(DISTINCT i.sku_id) AS unique_products,
                SUM(i.refund_value) AS total_refunds,
                ROUND(SUM(i.refund_value) / NULLIF(SUM(i.total_price), 0) * 100, 2) AS refund_rate_pct
            FROM ecommerce.silver.order_items i
            GROUP BY i.category
            ORDER BY total_revenue DESC
        """,
        "write_mode": "overwrite",
        "unique_key": "category",
        "schedule_type": "cron",
        "cron_schedule": "day",
    },
    {
        "domain": DOMAIN,
        "job_name": "cancellation_analysis",
        "query": """
            SELECT
                cancellation_reason,
                COUNT(*) AS total_canceled,
                SUM(total_value) AS lost_revenue,
                AVG(total_value) AS avg_canceled_order_value,
                COUNT(DISTINCT customer_id) AS affected_customers,
                COUNT(DISTINCT seller_id) AS affected_sellers,
                CAST(creation_date AS DATE) AS cancel_date
            FROM ecommerce.silver.orders
            WHERE status IN ('canceled', 'cancellation_requested')
                AND cancellation_reason != ''
            GROUP BY cancellation_reason, CAST(creation_date AS DATE)
            ORDER BY cancel_date, total_canceled DESC
        """,
        "write_mode": "overwrite",
        "unique_key": "cancellation_reason",
        "schedule_type": "cron",
        "cron_schedule": "day",
    },
    {
        "domain": DOMAIN,
        "job_name": "payment_methods_summary",
        "query": """
            SELECT
                p.payment_method,
                p.card_brand,
                COUNT(*) AS transaction_count,
                SUM(p.total_value) AS total_processed,
                AVG(p.total_value) AS avg_transaction_value,
                AVG(p.installments) AS avg_installments,
                COUNT(CASE WHEN p.payment_status = 'approved' THEN 1 END) AS approved_count,
                COUNT(CASE WHEN p.payment_status = 'pending' THEN 1 END) AS pending_count,
                COUNT(CASE WHEN p.payment_status = 'refunded' THEN 1 END) AS refunded_count,
                ROUND(
                    COUNT(CASE WHEN p.payment_status = 'approved' THEN 1 END) * 100.0
                    / COUNT(*), 2
                ) AS approval_rate_pct
            FROM ecommerce.silver.order_payments p
            GROUP BY p.payment_method, p.card_brand
            ORDER BY total_processed DESC
        """,
        "write_mode": "overwrite",
        "unique_key": "payment_method",
        "schedule_type": "cron",
        "cron_schedule": "day",
    },
    {
        "domain": DOMAIN,
        "job_name": "shipping_performance",
        "query": """
            SELECT
                s.carrier_name,
                s.shipping_method,
                COUNT(*) AS total_shipments,
                AVG(s.shipping_value) AS avg_shipping_cost,
                AVG(s.estimated_delivery_days) AS avg_estimated_days,
                AVG(s.weight_kg) AS avg_weight_kg,
                COUNT(CASE WHEN s.delivered_at != '' THEN 1 END) AS delivered_count,
                COUNT(CASE WHEN s.tracking_number != '' THEN 1 END) AS tracked_count,
                ROUND(
                    COUNT(CASE WHEN s.delivered_at != '' THEN 1 END) * 100.0
                    / NULLIF(COUNT(CASE WHEN s.shipped_at != '' THEN 1 END), 0), 2
                ) AS delivery_rate_pct
            FROM ecommerce.silver.order_shipping s
            GROUP BY s.carrier_name, s.shipping_method
            ORDER BY total_shipments DESC
        """,
        "write_mode": "overwrite",
        "unique_key": "carrier_name",
        "schedule_type": "cron",
        "cron_schedule": "day",
    },
    {
        "domain": DOMAIN,
        "job_name": "seller_performance",
        "query": """
            SELECT
                o.seller_id,
                o.seller_name,
                COUNT(DISTINCT o.order_id) AS total_orders,
                COUNT(DISTINCT o.customer_id) AS unique_customers,
                SUM(CASE WHEN o.status = 'invoiced' THEN o.total_value ELSE 0 END) AS total_revenue,
                AVG(CASE WHEN o.status = 'invoiced' THEN o.total_value END) AS avg_order_value,
                COUNT(CASE WHEN o.status = 'canceled' THEN 1 END) AS canceled_orders,
                ROUND(
                    COUNT(CASE WHEN o.status = 'canceled' THEN 1 END) * 100.0
                    / NULLIF(COUNT(*), 0), 2
                ) AS cancellation_rate_pct
            FROM ecommerce.silver.orders o
            GROUP BY o.seller_id, o.seller_name
            ORDER BY total_revenue DESC
        """,
        "write_mode": "overwrite",
        "unique_key": "seller_id",
        "schedule_type": "cron",
        "cron_schedule": "day",
    },
    {
        "domain": DOMAIN,
        "job_name": "state_revenue",
        "query": """
            SELECT
                o.shipping_state AS state_code,
                o.shipping_city AS city,
                COUNT(DISTINCT o.order_id) AS total_orders,
                COUNT(DISTINCT o.customer_id) AS unique_customers,
                SUM(CASE WHEN o.status = 'invoiced' THEN o.total_value ELSE 0 END) AS total_revenue,
                AVG(CASE WHEN o.status = 'invoiced' THEN o.total_value END) AS avg_order_value,
                AVG(CASE WHEN o.status = 'invoiced' THEN o.shipping_value END) AS avg_shipping_cost
            FROM ecommerce.silver.orders o
            GROUP BY o.shipping_state, o.shipping_city
            ORDER BY total_revenue DESC
        """,
        "write_mode": "overwrite",
        "unique_key": "state_code",
        "schedule_type": "cron",
        "cron_schedule": "day",
    },
    {
        "domain": DOMAIN,
        "job_name": "hourly_order_pattern",
        "query": """
            SELECT
                EXTRACT(HOUR FROM CAST(creation_date AS TIMESTAMP)) AS order_hour,
                EXTRACT(DOW FROM CAST(creation_date AS TIMESTAMP)) AS day_of_week,
                COUNT(*) AS total_orders,
                SUM(CASE WHEN status = 'invoiced' THEN total_value ELSE 0 END) AS revenue,
                AVG(total_value) AS avg_order_value,
                COUNT(DISTINCT customer_id) AS unique_customers
            FROM ecommerce.silver.orders
            GROUP BY
                EXTRACT(HOUR FROM CAST(creation_date AS TIMESTAMP)),
                EXTRACT(DOW FROM CAST(creation_date AS TIMESTAMP))
            ORDER BY day_of_week, order_hour
        """,
        "write_mode": "overwrite",
        "unique_key": "order_hour",
        "schedule_type": "cron",
        "cron_schedule": "day",
    },
    {
        "domain": DOMAIN,
        "job_name": "coupon_effectiveness",
        "query": """
            SELECT
                CASE WHEN coupon_code != '' THEN coupon_code ELSE 'no_coupon' END AS coupon,
                COUNT(*) AS times_used,
                SUM(total_value) AS total_revenue,
                SUM(discount_value) AS total_discount_given,
                AVG(total_value) AS avg_order_value,
                AVG(total_quantity) AS avg_items_per_order,
                COUNT(CASE WHEN status = 'invoiced' THEN 1 END) AS completed_orders,
                COUNT(CASE WHEN status = 'canceled' THEN 1 END) AS canceled_orders,
                ROUND(
                    COUNT(CASE WHEN status = 'invoiced' THEN 1 END) * 100.0
                    / NULLIF(COUNT(*), 0), 2
                ) AS conversion_rate_pct
            FROM ecommerce.silver.orders
            GROUP BY CASE WHEN coupon_code != '' THEN coupon_code ELSE 'no_coupon' END
            ORDER BY total_revenue DESC
        """,
        "write_mode": "overwrite",
        "unique_key": "coupon",
        "schedule_type": "cron",
        "cron_schedule": "day",
    },
]


# =============================================================================
# Sample Queries for the Query API
# =============================================================================

SAMPLE_QUERIES = [
    {
        "name": "Total Revenue (Invoiced Orders)",
        "sql": "SELECT SUM(total_value) AS total_revenue, COUNT(*) AS total_invoiced FROM ecommerce.silver.orders WHERE status = 'invoiced'",
    },
    {
        "name": "Orders by Status",
        "sql": "SELECT status, COUNT(*) AS count, ROUND(SUM(total_value), 2) AS total_value FROM ecommerce.silver.orders GROUP BY status ORDER BY count DESC",
    },
    {
        "name": "Top 10 Products by Revenue",
        "sql": "SELECT product_name, category, SUM(quantity) AS units_sold, ROUND(SUM(total_price), 2) AS revenue FROM ecommerce.silver.order_items GROUP BY product_name, category ORDER BY revenue DESC LIMIT 10",
    },
    {
        "name": "Revenue by Category",
        "sql": "SELECT category, COUNT(DISTINCT order_id) AS orders, SUM(quantity) AS units, ROUND(SUM(total_price), 2) AS revenue FROM ecommerce.silver.order_items GROUP BY category ORDER BY revenue DESC",
    },
    {
        "name": "Cancellation Reasons",
        "sql": "SELECT cancellation_reason, COUNT(*) AS count, ROUND(SUM(total_value), 2) AS lost_revenue FROM ecommerce.silver.orders WHERE status IN ('canceled', 'cancellation_requested') AND cancellation_reason != '' GROUP BY cancellation_reason ORDER BY count DESC",
    },
    {
        "name": "Payment Method Distribution",
        "sql": "SELECT payment_method, COUNT(*) AS count, ROUND(SUM(total_value), 2) AS total, ROUND(AVG(installments), 1) AS avg_installments FROM ecommerce.silver.order_payments GROUP BY payment_method ORDER BY total DESC",
    },
    {
        "name": "Revenue by State",
        "sql": "SELECT shipping_state, COUNT(*) AS orders, ROUND(SUM(total_value), 2) AS revenue FROM ecommerce.silver.orders WHERE status = 'invoiced' GROUP BY shipping_state ORDER BY revenue DESC",
    },
    {
        "name": "Daily Revenue Trend",
        "sql": "SELECT CAST(creation_date AS DATE) AS dt, COUNT(*) AS orders, ROUND(SUM(total_value), 2) AS revenue FROM ecommerce.silver.orders WHERE status = 'invoiced' GROUP BY CAST(creation_date AS DATE) ORDER BY dt",
    },
    {
        "name": "Top 10 Customers by Spend",
        "sql": "SELECT customer_id, customer_name, COUNT(*) AS orders, ROUND(SUM(total_value), 2) AS total_spent FROM ecommerce.silver.orders WHERE status = 'invoiced' GROUP BY customer_id, customer_name ORDER BY total_spent DESC LIMIT 10",
    },
    {
        "name": "Seller Performance",
        "sql": "SELECT seller_name, COUNT(*) AS orders, ROUND(SUM(total_value), 2) AS revenue, COUNT(CASE WHEN status = 'canceled' THEN 1 END) AS cancels FROM ecommerce.silver.orders GROUP BY seller_name ORDER BY revenue DESC",
    },
]


# =============================================================================
# API Client
# =============================================================================

class DataLakeClient:
    """Client for the Serverless Data Lake APIs."""

    def __init__(self, base_url: str, timeout: int = 30):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.timeout = timeout
        self.stats = {
            "requests": 0,
            "errors": 0,
            "total_records_sent": 0,
            "request_times": [],
        }

    def _request(self, method: str, path: str, **kwargs) -> requests.Response:
        """Make an HTTP request with timing and error tracking."""
        url = f"{self.base_url}{path}"
        kwargs.setdefault("timeout", self.timeout)
        start = time.time()
        try:
            resp = self.session.request(method, url, **kwargs)
            elapsed = time.time() - start
            self.stats["requests"] += 1
            self.stats["request_times"].append(elapsed)
            if resp.status_code >= 400:
                self.stats["errors"] += 1
            return resp
        except requests.RequestException as e:
            self.stats["requests"] += 1
            self.stats["errors"] += 1
            raise

    # --- Endpoints API ---
    def create_endpoint(self, schema: dict) -> dict:
        resp = self._request("POST", "/endpoints", json=schema)
        resp.raise_for_status()
        return resp.json()

    def list_endpoints(self, domain: str | None = None) -> list[dict]:
        params = {"domain": domain} if domain else {}
        resp = self._request("GET", "/endpoints", params=params)
        resp.raise_for_status()
        return resp.json()

    def delete_endpoint(self, domain: str, name: str) -> dict:
        resp = self._request("DELETE", f"/endpoints/{domain}/{name}")
        return resp.json()

    # --- Ingestion API ---
    def ingest_single(self, domain: str, endpoint: str, data: dict) -> dict:
        resp = self._request(
            "POST", f"/ingest/{domain}/{endpoint}",
            json={"data": data},
            params={"validate": "true", "strict": "false"},
        )
        resp.raise_for_status()
        self.stats["total_records_sent"] += 1
        return resp.json()

    def ingest_batch(self, domain: str, endpoint: str, records: list[dict]) -> dict:
        resp = self._request(
            "POST", f"/ingest/{domain}/{endpoint}/batch",
            json=records,
            params={"validate": "true", "strict": "false"},
        )
        resp.raise_for_status()
        self.stats["total_records_sent"] += len(records)
        return resp.json()

    # --- Transform Jobs API ---
    def create_transform_job(self, job: dict) -> dict:
        resp = self._request("POST", "/transform/jobs", json=job)
        resp.raise_for_status()
        return resp.json()

    def list_transform_jobs(self, domain: str | None = None) -> list[dict]:
        params = {"domain": domain} if domain else {}
        resp = self._request("GET", "/transform/jobs", params=params)
        resp.raise_for_status()
        return resp.json()

    def run_transform_job(self, domain: str, job_name: str) -> dict:
        resp = self._request("POST", f"/transform/jobs/{domain}/{job_name}/run")
        resp.raise_for_status()
        return resp.json()

    def delete_transform_job(self, domain: str, job_name: str) -> dict:
        resp = self._request("DELETE", f"/transform/jobs/{domain}/{job_name}")
        return resp.json()

    # --- Query API ---
    def query(self, sql: str) -> dict:
        resp = self._request("GET", "/consumption/query", params={"sql": sql})
        resp.raise_for_status()
        return resp.json()

    def list_tables(self) -> dict:
        resp = self._request("GET", "/consumption/tables")
        resp.raise_for_status()
        return resp.json()

    def get_stats_summary(self) -> dict:
        times = self.stats["request_times"]
        return {
            "total_requests": self.stats["requests"],
            "total_errors": self.stats["errors"],
            "total_records_sent": self.stats["total_records_sent"],
            "error_rate_pct": round(self.stats["errors"] / max(self.stats["requests"], 1) * 100, 2),
            "avg_response_time_ms": round(sum(times) / max(len(times), 1) * 1000, 1),
            "p50_response_time_ms": round(sorted(times)[len(times) // 2] * 1000, 1) if times else 0,
            "p95_response_time_ms": round(sorted(times)[int(len(times) * 0.95)] * 1000, 1) if times else 0,
            "p99_response_time_ms": round(sorted(times)[int(len(times) * 0.99)] * 1000, 1) if times else 0,
            "max_response_time_ms": round(max(times) * 1000, 1) if times else 0,
        }


# =============================================================================
# Load Test Runner
# =============================================================================

class LoadTestRunner:
    """Orchestrates the end-to-end load test."""

    def __init__(self, client: DataLakeClient, generator: EcommerceDataGenerator,
                 batch_size: int = 25, max_workers: int = 5):
        self.client = client
        self.generator = generator
        self.batch_size = batch_size
        self.max_workers = max_workers

    def _print_header(self, title: str) -> None:
        width = 70
        print(f"\n{'=' * width}")
        print(f"  {title}")
        print(f"{'=' * width}")

    def _print_step(self, step: str) -> None:
        print(f"\n  >> {step}")

    def _print_result(self, label: str, value: Any) -> None:
        print(f"     {label}: {value}")

    # -------------------------------------------------------------------------
    # Phase 1: Setup endpoints
    # -------------------------------------------------------------------------
    def phase_setup(self) -> None:
        self._print_header("PHASE 1: SETUP - Creating Endpoint Schemas")

        for name, schema in ENDPOINT_SCHEMAS.items():
            self._print_step(f"Creating endpoint: {DOMAIN}/{name}")
            try:
                result = self.client.create_endpoint(schema)
                self._print_result("ID", result.get("id"))
                self._print_result("Endpoint URL", result.get("endpoint_url"))
                self._print_result("Version", result.get("version"))
            except requests.HTTPError as e:
                if e.response.status_code == 400 and "already exists" in e.response.text.lower():
                    self._print_result("Status", "Already exists (skipping)")
                else:
                    self._print_result("Error", f"{e.response.status_code} - {e.response.text}")

        # Verify
        self._print_step("Verifying created endpoints")
        endpoints = self.client.list_endpoints(domain=DOMAIN)
        self._print_result("Total endpoints in domain", len(endpoints))
        for ep in endpoints:
            self._print_result(f"  {ep['name']}", f"v{ep['version']} - {ep['status']}")

    # -------------------------------------------------------------------------
    # Phase 2: Generate and ingest data
    # -------------------------------------------------------------------------
    def phase_ingest(self) -> None:
        self._print_header("PHASE 2: DATA GENERATION & INGESTION")

        # Generate data
        self._print_step("Generating e-commerce data...")
        self.generator.generate_all()
        summary = self.generator.get_summary()

        self._print_result("Total orders", summary["total_orders"])
        self._print_result("Total line items", summary["total_items"])
        self._print_result("Total payments", summary["total_payments"])
        self._print_result("Total shipping records", summary["total_shipping"])
        self._print_result("Unique customers", summary["unique_customers"])
        self._print_result("Date range", summary["date_range"])
        self._print_result("Total revenue (invoiced)", f"R$ {summary['total_revenue_brl']:,.2f}")
        self._print_result("Avg order value (invoiced)", f"R$ {summary['avg_order_value']:,.2f}")
        self._print_result("Total items sold", summary["total_items_sold"])
        print("\n     Status distribution:")
        for status, count in sorted(summary["statuses"].items(), key=lambda x: -x[1]):
            pct = count / summary["total_orders"] * 100
            self._print_result(f"    {status}", f"{count} ({pct:.1f}%)")

        # Ingest all datasets
        datasets = [
            ("orders", self.generator.orders),
            ("order_items", self.generator.order_items),
            ("order_payments", self.generator.order_payments),
            ("order_shipping", self.generator.order_shipping),
        ]

        for endpoint_name, records in datasets:
            self._ingest_dataset(endpoint_name, records)

    def _ingest_dataset(self, endpoint_name: str, records: list[dict]) -> None:
        """Ingest a dataset using batch API with concurrent requests."""
        self._print_step(f"Ingesting {len(records)} records into {DOMAIN}/{endpoint_name}")

        # Split into batches
        batches = [
            records[i:i + self.batch_size]
            for i in range(0, len(records), self.batch_size)
        ]

        sent = 0
        errors = 0
        start_time = time.time()

        def send_batch(batch: list[dict]) -> dict | None:
            try:
                return self.client.ingest_batch(DOMAIN, endpoint_name, batch)
            except Exception as e:
                return {"error": str(e)}

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(send_batch, b): b for b in batches}
            for future in as_completed(futures):
                result = future.result()
                if result and "error" not in result:
                    sent += result.get("sent_count", 0)
                else:
                    errors += 1
                    err_msg = result.get("error", "Unknown") if result else "None"
                    print(f"       [ERROR] Batch failed: {err_msg}")

                # Progress
                done = sent + errors * self.batch_size
                if done % (self.batch_size * 10) == 0 or done >= len(records):
                    elapsed = time.time() - start_time
                    rate = sent / max(elapsed, 0.001)
                    print(f"       Progress: {sent}/{len(records)} sent | "
                          f"{errors} batch errors | {rate:.0f} records/s")

        elapsed = time.time() - start_time
        self._print_result("Records sent", sent)
        self._print_result("Batch errors", errors)
        self._print_result("Elapsed", f"{elapsed:.1f}s")
        self._print_result("Throughput", f"{sent / max(elapsed, 0.001):.0f} records/s")

    # -------------------------------------------------------------------------
    # Phase 3: Create transform jobs
    # -------------------------------------------------------------------------
    def phase_transform(self) -> None:
        self._print_header("PHASE 3: GOLD LAYER - Creating Transform Jobs")

        for job in TRANSFORM_JOBS:
            self._print_step(f"Creating job: {job['domain']}/{job['job_name']}")
            try:
                result = self.client.create_transform_job(job)
                self._print_result("ID", result.get("id"))
                self._print_result("Write mode", result.get("write_mode"))
                self._print_result("Schedule", result.get("cron_schedule"))
            except requests.HTTPError as e:
                if e.response.status_code == 400 and "already exists" in e.response.text.lower():
                    self._print_result("Status", "Already exists (skipping)")
                else:
                    self._print_result("Error", f"{e.response.status_code} - {e.response.text}")

        # List all jobs
        self._print_step("Verifying created transform jobs")
        jobs = self.client.list_transform_jobs(domain=DOMAIN)
        self._print_result("Total jobs", len(jobs))
        for j in jobs:
            self._print_result(f"  {j['job_name']}", f"{j['write_mode']} / {j.get('cron_schedule', 'N/A')}")

    # -------------------------------------------------------------------------
    # Phase 4: Query data
    # -------------------------------------------------------------------------
    def phase_query(self) -> None:
        self._print_header("PHASE 4: QUERYING - Running Analytics Queries")

        self._print_step("Listing available tables")
        try:
            tables = self.client.list_tables()
            self._print_result("Tables found", tables.get("count", 0))
            for t in tables.get("tables", []):
                self._print_result(f"  {t['domain']}.silver.{t['name']}", f"{len(t.get('columns', []))} columns")
        except Exception as e:
            self._print_result("Error listing tables", str(e))

        for q in SAMPLE_QUERIES:
            self._print_step(f"Query: {q['name']}")
            print(f"     SQL: {q['sql'][:100]}{'...' if len(q['sql']) > 100 else ''}")
            try:
                start = time.time()
                result = self.client.query(q["sql"])
                elapsed = (time.time() - start) * 1000
                self._print_result("Rows returned", result.get("row_count", 0))
                self._print_result("Response time", f"{elapsed:.0f}ms")
                # Print first 3 rows as sample
                for row in result.get("data", [])[:3]:
                    self._print_result("  ", json.dumps(row, default=str, ensure_ascii=False))
                if result.get("row_count", 0) > 3:
                    self._print_result("  ", f"... and {result['row_count'] - 3} more rows")
            except Exception as e:
                self._print_result("Error", str(e))

    # -------------------------------------------------------------------------
    # Run all phases
    # -------------------------------------------------------------------------
    def run(self, phases: list[str] | None = None) -> None:
        all_phases = ["setup", "ingest", "transform", "query"]
        phases = phases or all_phases

        self._print_header("SERVERLESS DATA LAKE - E-COMMERCE LOAD TEST")
        print(f"  Base URL:    {self.client.base_url}")
        print(f"  Orders:      {self.generator.num_orders}")
        print(f"  Batch size:  {self.batch_size}")
        print(f"  Workers:     {self.max_workers}")
        print(f"  Phases:      {', '.join(phases)}")

        start = time.time()

        if "setup" in phases:
            self.phase_setup()
        if "ingest" in phases:
            self.phase_ingest()
        if "transform" in phases:
            self.phase_transform()
        if "query" in phases:
            self.phase_query()

        elapsed = time.time() - start

        # Final summary
        self._print_header("LOAD TEST COMPLETE - SUMMARY")
        stats = self.client.get_stats_summary()
        self._print_result("Total elapsed", f"{elapsed:.1f}s")
        self._print_result("Total HTTP requests", stats["total_requests"])
        self._print_result("Total records ingested", stats["total_records_sent"])
        self._print_result("Error rate", f"{stats['error_rate_pct']}%")
        self._print_result("Avg response time", f"{stats['avg_response_time_ms']}ms")
        self._print_result("P50 response time", f"{stats['p50_response_time_ms']}ms")
        self._print_result("P95 response time", f"{stats['p95_response_time_ms']}ms")
        self._print_result("P99 response time", f"{stats['p99_response_time_ms']}ms")
        self._print_result("Max response time", f"{stats['max_response_time_ms']}ms")


# =============================================================================
# Cleanup helper
# =============================================================================

def cleanup(client: DataLakeClient) -> None:
    """Remove all endpoints and transform jobs created by this load test."""
    print("\n  Cleaning up...")

    for name in ENDPOINT_SCHEMAS:
        try:
            client.delete_endpoint(DOMAIN, name)
            print(f"    Deleted endpoint: {DOMAIN}/{name}")
        except Exception:
            pass

    for job in TRANSFORM_JOBS:
        try:
            client.delete_transform_job(job["domain"], job["job_name"])
            print(f"    Deleted job: {job['domain']}/{job['job_name']}")
        except Exception:
            pass

    print("  Cleanup complete.")


# =============================================================================
# CLI Entry Point
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="E-commerce Load Test for Serverless Data Lake",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full end-to-end test with 2000 orders
  python scripts/load_test_ecommerce.py --base-url https://<api>.execute-api.us-east-1.amazonaws.com

  # Large volume test
  python scripts/load_test_ecommerce.py --base-url https://... --num-orders 10000 --batch-size 100 --workers 10

  # Dry run - just generate and print sample data
  python scripts/load_test_ecommerce.py --dry-run --num-orders 5

  # Run only ingestion phase (endpoints already created)
  python scripts/load_test_ecommerce.py --base-url https://... --phase ingest

  # Run only query phase (data already ingested)
  python scripts/load_test_ecommerce.py --base-url https://... --phase query

  # Cleanup all created resources
  python scripts/load_test_ecommerce.py --base-url https://... --cleanup
        """,
    )

    parser.add_argument(
        "--base-url", type=str, default=None,
        help="API Gateway base URL (e.g., https://<id>.execute-api.<region>.amazonaws.com)",
    )
    parser.add_argument(
        "--num-orders", type=int, default=2000,
        help="Number of orders to generate (default: 2000)",
    )
    parser.add_argument(
        "--batch-size", type=int, default=25,
        help="Records per batch ingestion request (default: 25)",
    )
    parser.add_argument(
        "--workers", type=int, default=5,
        help="Concurrent threads for ingestion (default: 5)",
    )
    parser.add_argument(
        "--start-date", type=str, default="2024-07-01",
        help="Start date for generated orders (default: 2024-07-01)",
    )
    parser.add_argument(
        "--end-date", type=str, default="2025-01-31",
        help="End date for generated orders (default: 2025-01-31)",
    )
    parser.add_argument(
        "--phase", type=str, action="append", default=None,
        choices=["setup", "ingest", "transform", "query"],
        help="Run specific phase(s). Can be repeated. Default: all phases.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Generate data and print samples without sending to API",
    )
    parser.add_argument(
        "--cleanup", action="store_true",
        help="Delete all endpoints and transform jobs created by this script",
    )
    parser.add_argument(
        "--timeout", type=int, default=30,
        help="HTTP request timeout in seconds (default: 30)",
    )
    parser.add_argument(
        "--seed", type=int, default=None,
        help="Random seed for reproducible data generation",
    )

    args = parser.parse_args()

    # Set random seed if provided
    if args.seed is not None:
        random.seed(args.seed)

    # Dry run mode
    if args.dry_run:
        print("=" * 70)
        print("  DRY RUN - Generating sample data")
        print("=" * 70)
        gen = EcommerceDataGenerator(
            num_orders=args.num_orders,
            start_date=args.start_date,
            end_date=args.end_date,
        )
        gen.generate_all()
        summary = gen.get_summary()

        print(f"\n  Summary: {json.dumps(summary, indent=2, default=str, ensure_ascii=False)}")

        print("\n  --- Sample Order ---")
        print(json.dumps(gen.orders[0], indent=2, default=str, ensure_ascii=False))

        print("\n  --- Sample Order Items ---")
        sample_order_id = gen.orders[0]["order_id"]
        items = [i for i in gen.order_items if i["order_id"] == sample_order_id]
        for item in items:
            print(json.dumps(item, indent=2, default=str, ensure_ascii=False))

        print("\n  --- Sample Payment ---")
        payment = [p for p in gen.order_payments if p["order_id"] == sample_order_id][0]
        print(json.dumps(payment, indent=2, default=str, ensure_ascii=False))

        print("\n  --- Sample Shipping ---")
        shipping = [s for s in gen.order_shipping if s["order_id"] == sample_order_id][0]
        print(json.dumps(shipping, indent=2, default=str, ensure_ascii=False))

        print(f"\n  Endpoint schemas that would be created: {list(ENDPOINT_SCHEMAS.keys())}")
        print(f"  Transform jobs that would be created: {[j['job_name'] for j in TRANSFORM_JOBS]}")
        return

    # Require base_url for non-dry-run
    if not args.base_url:
        parser.error("--base-url is required (unless using --dry-run)")

    client = DataLakeClient(base_url=args.base_url, timeout=args.timeout)

    # Cleanup mode
    if args.cleanup:
        cleanup(client)
        return

    # Run load test
    generator = EcommerceDataGenerator(
        num_orders=args.num_orders,
        start_date=args.start_date,
        end_date=args.end_date,
    )

    runner = LoadTestRunner(
        client=client,
        generator=generator,
        batch_size=args.batch_size,
        max_workers=args.workers,
    )

    runner.run(phases=args.phase)


if __name__ == "__main__":
    main()
