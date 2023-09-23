class Mov(object):
    def __init__(self, movimientoId=None, productoId=None, fechaOperacion=None, importe=None, concepto=None, descripcionOperacion=None, categoriaId=None, marcaComercial=None):
        self.movimientoId = movimientoId
        self.productoId = productoId
        self.fechaOperacion = fechaOperacion
        self.importe = importe
        self.concepto = concepto
        self.descripcionOperacion = descripcionOperacion
        self.categoriaId = categoriaId
        self.marcaComercial = marcaComercial