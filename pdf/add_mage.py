"""
PDF Image Insertion Script
Adds images to specific positions in a PDF document
"""

from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from PyPDF2 import PdfReader, PdfWriter
from reportlab.lib.utils import ImageReader
import io

def add_images_to_pdf(input_pdf, output_pdf, image_configs):
    """
    Add images to a PDF at specified positions
    
    Args:
        input_pdf: Path to input PDF file
        output_pdf: Path to output PDF file
        image_configs: List of dicts with image configuration
            Each dict should have:
            - 'page': Page number (0-indexed)
            - 'image_path': Path to image file
            - 'x': X coordinate (from bottom-left)
            - 'y': Y coordinate (from bottom-left)
            - 'width': Image width
            - 'height': Image height
    """
    
    # Read the original PDF
    reader = PdfReader(input_pdf)
    writer = PdfWriter()
    
    # Group images by page for efficiency
    images_by_page = {}
    for config in image_configs:
        page_num = config['page']
        if page_num not in images_by_page:
            images_by_page[page_num] = []
        images_by_page[page_num].append(config)
    
    # Process each page
    for page_num in range(len(reader.pages)):
        page = reader.pages[page_num]
        
        # If this page has images to add
        if page_num in images_by_page:
            # Create a new PDF with the images
            packet = io.BytesIO()
            
            # Get page dimensions
            page_width = float(page.mediabox.width)
            page_height = float(page.mediabox.height)
            
            # Create canvas with same dimensions
            can = canvas.Canvas(packet, pagesize=(page_width, page_height))
            
            # Add each image for this page
            for img_config in images_by_page[page_num]:
                try:
                    can.drawImage(
                        img_config['image_path'],
                        img_config['x'],
                        img_config['y'],
                        width=img_config['width'],
                        height=img_config['height'],
                        preserveAspectRatio=True,
                        mask='auto'
                    )
                except Exception as e:
                    print(f"Error adding image {img_config['image_path']}: {e}")
            
            can.save()
            
            # Move to the beginning of the BytesIO buffer
            packet.seek(0)
            
            # Read the new PDF with images
            overlay_pdf = PdfReader(packet)
            overlay_page = overlay_pdf.pages[0]
            
            # Merge the overlay with the original page
            page.merge_page(overlay_page)
        
        # Add the page to the output
        writer.add_page(page)
    
    # Write the output PDF
    with open(output_pdf, 'wb') as output_file:
        writer.write(output_file)
    
    print(f"PDF created successfully: {output_pdf}")


# Example usage
if __name__ == "__main__":
    # Configuration for images to add
    # Coordinates are from bottom-left corner of the page
    image_configs = [
        {
            'page': 0,  # First page (0-indexed)
            'image_path': 'P.png',
            'x': 320,  # X position from left
            'y': 660,  # Y position from bottom
            'width': 100,
            'height': 50
        },
        # {
        #     'page': 0,  # First page
        #     'image_path': 'image2.jpg',
        #     'x': 350,
        #     'y': 500,
        #     'width': 200,
        #     'height': 150
        # },
        # {
        #     'page': 1,  # Second page
        #     'image_path': 'image3.png',
        #     'x': 100,
        #     'y': 300,
        #     'width': 150,
        #     'height': 150
        # }
    ]
    
    # Process the PDF
    add_images_to_pdf(
        input_pdf='k.pdf',
        output_pdf='output_with_images.pdf',
        image_configs=image_configs,
    )
    
    print("\nNote: Adjust x, y coordinates based on your PDF layout")
    print("- x: distance from left edge")
    print("- y: distance from bottom edge")
    print("- Standard letter page is 612 x 792 points")