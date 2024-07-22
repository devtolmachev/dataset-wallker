import flet as ft


def main(page: ft.Page):
    c = ft.Container(
        width=150,
        height=150,
        bgcolor="blue",
        border_radius=10,
        offset=ft.Offset(0, 0),
        animate_offset=ft.animation.Animation(
            1000, ft.animation.AnimationCurve.EASE_IN_OUT_QUINT
        ),
    )
    row = ft.Row(
        controls=[
            ft.Container(
                width=150,
                height=150,
                bgcolor="blue",
                border_radius=10,
                offset=ft.Offset(i, 0),
                animate_offset=ft.animation.Animation(
                    1000, ft.animation.AnimationCurve.EASE_IN_CIRC
                ),
            )
            for i in range(0, 3)
        ],
    )

    def animate(e):
        for c in row.controls:
            c.offset = ft.Offset(c.offset.x + 0.8, 0)

        page.update()

    page.add(
        row,
        ft.ElevatedButton("Reveal!", on_click=animate),
    )


ft.app(target=main)
