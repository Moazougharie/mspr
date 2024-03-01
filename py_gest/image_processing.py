from PIL import Image
import io

class ImageProcessor:
    @staticmethod
    def resize_image(image_bytes, size=(600, 800)):
        # Convertir les bytes en image
        image = Image.open(io.BytesIO(image_bytes))
        # Redimensionner l'image
        image_resized = image.resize(size)
        # Convertir l'image redimensionnée en bytes
        img_byte_arr = io.BytesIO()
        image_resized.save(img_byte_arr, format='JPEG')
        return img_byte_arr.getvalue()
